import base64
import datetime as dt
import requests as rq

from dateutil import parser


def refresh_fitbit_token(request):
    """Refresh an OAuth2 "Authorization Code Grant Flow" refresh token.

    See https://dev.fitbit.com/build/reference/web-api/oauth2/#refreshing-tokens

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

    # see https://dev.fitbit.com/build/reference/web-api/oauth2/#refreshing-tokens  # noqa
    # for why we have to do this base64 stuff
    id_secret = (f'{request_json["secrets"]["FITBIT_CLIENT_ID"]}:'
                 f'{request_json["secrets"]["FITBIT_CLIENT_SECRET"]}')

    b64_creds = (base64.encodebytes(bytes(id_secret, 'utf8'))
                 .decode('utf8')
                 .rstrip())

    auth_header = {'Authorization': f'Basic {b64_creds}',
                   'Content-Type': 'application/x-www-form-urlencoded'}

    post_params = {'grant_type': 'refresh_token',
                   'refresh_token': request_json['state']['refresh_token']}

    resp = rq.post('https://api.fitbit.com/oauth2/token',
                   headers=auth_header, params=post_params)

    if resp.status_code != 200:
        raise ValueError(f'OAuth token refresh request returned {resp.json()}')

    new_token = resp.json()

    return new_token


def handler(request):
    """Scrape data from Fitbit.

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
    if 'cursor' not in request_json['state']:
        request_json['state']['cursor'] = request_json['secrets']['cursor']
    if 'access_token' not in request_json['state']:
        request_json['state']['access_token'] = request_json['secrets']['access_token']
    if 'refresh_token' not in request_json['state']:
        request_json['state']['refresh_token'] = request_json['secrets']['refresh_token']

    cursor = request_json['state']['cursor']
    cursor_date = parser.parse(cursor).date()

    if cursor_date > dt.date.today():
        raise ValueError(
            f"cursor value {cursor_date.isoformat()} is later than "
            f"today's date {dt.date.today().isoformat()}")

    # if the cursor is at the current date return immediately without
    # incrementing the cursor. This is to ensure we don't pull data for a day
    # until that day is over.
    if cursor_date == dt.date.today():
        return {
            'state': request_json['state'],
            'hasMore': False,
            'returnCause': 'Cursor date not complete yet',
        }

    # otherwise the cursor must be in the past so go ahead and pull data
    headers = {'Accept-Language': 'en_US',
               'Authorization': f'Bearer {request_json["state"]["access_token"]}'}

    activity = rq.get(
        'https://api.fitbit.com/1/user/-/activities/date/'
        f'{cursor_date.isoformat()}.json',
        headers=headers)
    weight = rq.get(
        'https://api.fitbit.com/1/user/-/body/log/weight/date/'
        f'{cursor_date.isoformat()}.json',
        headers=headers)

    # the fitbit API returns a 401 code when the access token has expired
    # see https://dev.fitbit.com/build/reference/web-api/oauth2/#refreshing-tokens
    if activity.status_code == 401 or weight.status_code == 401:

        new_token = refresh_fitbit_token(request)

        return {
            'state': {
                'cursor': cursor_date.isoformat(),
                'access_token': new_token['access_token'],
                'refresh_token': new_token['refresh_token']
            },
            'hasMore': True,
            'returnCause': 'OAuth token required refresh',
        }

    # the fitbit API returns a 429 code when you hit the rate limit
    # see https://dev.fitbit.com/build/reference/web-api/basics/#hitting-the-rate-limit
    if activity.status_code == 429 or weight.status_code == 429:

        return {
            'state': request_json['state'],
            'hasMore': False,
            'returnCause': 'Hit rate limit',
        }

    # parse the response from the fitbit API
    activity_json = activity.json()
    weight_json = weight.json()

    activity_insert = {
        'date': cursor_date.isoformat(),
        'steps': activity_json['summary']['steps'],
        'caloriesBMR': activity_json['summary']['caloriesBMR'],
        'caloriesOut': activity_json['summary']['caloriesOut'],
        'activityCalories': activity_json['summary']['activityCalories'],
        'marginalCalories': activity_json['summary']['marginalCalories'],
        'sedentaryMinutes': activity_json['summary']['sedentaryMinutes'],
        'lightlyActiveMinutes': activity_json['summary']['lightlyActiveMinutes'],
        'fairlyActiveMinutes': activity_json['summary']['fairlyActiveMinutes'],
        'veryActiveMinutes': activity_json['summary']['veryActiveMinutes'],
    }

    weight_insert = {
        'date': cursor_date.isoformat(),
        'weight': weight_json['weight'][0]['weight'] if len(weight_json['weight']) > 0 else None
    }

    return {
        'state': {
            'cursor': (cursor_date + dt.timedelta(days=1)).isoformat(),
            'access_token': request_json['state']['access_token'],
            'refresh_token': request_json['state']['refresh_token'],
            'returnCause': 'Successfully retrieved data',
        },
        'insert': {
            'activity': [activity_insert],
            'weight': [weight_insert]
        },
        'schema': {
            'activity': {
                'primary_key': ['date']
            },
            'weight': {
                'primary_key': ['date']
            }
        },
        'hasMore': True
    }
