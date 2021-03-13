import datetime as dt
import requests as rq

from dateutil import parser, tz
from requests.auth import HTTPBasicAuth


def refresh_oauth_token(request):
    """Refresh an OAuth2 "Authorization Code Grant Flow" refresh token.

    Parameters
    ----------
    request: flask.Request
        GCP Cloud Function request context. The request will match this spec:
        https://fivetran.com/docs/functions#requestformat

    Returns
    -------
    dict
        A new refresh and access token pair
    """

    request_json = request.get_json()

    content_header = {"Content-Type": "application/x-www-form-urlencoded"}

    post_params = {
        "grant_type": "refresh_token",
        "refresh_token": request_json["secrets"]["refresh_token"],
    }

    resp = rq.post(
        "https://oauth2.googleapis.com/token",
        headers=content_header,
        params=post_params,
        auth=HTTPBasicAuth(
            request_json["secrets"]["GFIT_CLIENT_ID"],
            request_json["secrets"]["GFIT_CLIENT_SECRET"],
        ),
    )

    if resp.status_code != 200:
        raise ValueError(f"OAuth token refresh request returned {resp.json()}")

    new_token = resp.json()

    return new_token


def handler(request):
    """Scrape data from Google Fit.

    Parameters
    ----------
    request: flask.Request
        GCP Cloud Function request context. The request will match this spec:
        https://fivetran.com/docs/functions#requestformat

    Returns
    -------
    dict
        A response matching this spec:
        https://fivetran.com/docs/functions#responseformat
    """

    request_json = request.get_json()

    # initialize state for the case when fivetran is starting from scratch.
    # put initial values for the cursor and tokens in the 'secrets' node.
    # fivetran should automatically keep track of subsequent updates in the
    # 'state' node.
    if "cursor" not in request_json["state"]:
        request_json["state"]["cursor"] = request_json["secrets"]["cursor"]
    if "access_token" not in request_json["state"]:
        request_json["state"]["access_token"] = request_json["secrets"]["access_token"]

    cursor = request_json["state"]["cursor"]
    cursor_date = parser.parse(cursor).date()

    if cursor_date > dt.date.today():
        raise ValueError(
            f"cursor value {cursor_date.isoformat()} is later than "
            f"today's date {dt.date.today().isoformat()}"
        )

    # if the cursor is at the current date return immediately without
    # incrementing the cursor. This is to ensure we don't pull data for a day
    # until that day is over.
    if cursor_date == dt.date.today():
        return {
            "state": {
                "cursor": cursor,
            },
            "hasMore": False,
        }

    # otherwise the cursor must be in the past so go ahead and pull data
    headers = {
        "Accept-Language": "en_US",
        "Authorization": f'Bearer {request_json["state"]["access_token"]}',
    }

    # control pagination with startTime and endTime parameters which have to be
    # RFC3339 timestamps. in this case this means adding a timezone. See:
    # https://developers.google.com/fit/rest/v1/reference/users/sessions/list#parameters
    start_date = cursor_date
    end_date = cursor_date + dt.timedelta(days=1)

    params = {
        # hardcoding 'America/Chicago' timezone because that makes the date
        # cursor intuitive for my data
        "startTime": dt.datetime(
            start_date.year,
            start_date.month,
            start_date.day,
            tzinfo=tz.gettz("America/Chicago"),
        ).isoformat(),
        "endTime": dt.datetime(
            end_date.year,
            end_date.month,
            end_date.day,
            tzinfo=tz.gettz("America/Chicago"),
        ).isoformat(),
    }

    sessions = rq.get(
        "https://www.googleapis.com/fitness/v1/users/me/sessions",
        headers=headers,
        params=params,
    )

    # the google fit API returns a 401 code when the access token has expired
    # see https://dev.fitbit.com/build/reference/web-api/oauth2/#refreshing-tokens
    if sessions.status_code == 401:

        new_token = refresh_oauth_token(request)

        # apparently the way Google APIs do OAuth the refresh token never
        # expires and so doesn't need to be refreshed itself
        # c.f. https://developers.google.com/identity/protocols/oauth2/web-server#offline
        return {
            "state": {
                "cursor": cursor_date.isoformat(),
                "access_token": new_token["access_token"],
            },
            "hasMore": True,
        }

    # parse the response from the Google Fit API
    sessions_json = sessions.json()

    sessions_insert = [
        {
            "date": cursor_date.isoformat(),
            "id": sess["id"],
            "name": sess["name"],
            "description": sess["description"],
            "start_time_millis": sess["startTimeMillis"],
            "end_time_millis": sess["endTimeMillis"],
            "modified_time_millis": sess["modifiedTimeMillis"],
            "source": sess["application"]["packageName"],
        }
        for sess in sessions_json["session"]
    ]

    return {
        "state": {
            "cursor": (cursor_date + dt.timedelta(days=1)).isoformat(),
            "access_token": request_json["state"]["access_token"],
        },
        "insert": {"sessions": sessions_insert if sessions_insert else None},
        "schema": {
            "sessions": {"primary_key": ["date", "id"]},
        },
        "hasMore": True,
    }
