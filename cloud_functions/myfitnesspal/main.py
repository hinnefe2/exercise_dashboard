import datetime as dt
import myfitnesspal as mfp

from dateutil import parser


def handler(request):
    """Scrapes data from MyFitnessPal.

    Parameters
    ----------
    request: dict
        GCP Cloud Function request context. The request will match this spec:
        https://fivetran.com/docs/functions#requestformat

    Returns
    -------
    dict
        A response matching this spec:
        https://fivetran.com/docs/functions#responseformat
    """

    request_json = request.get_json()

    client = mfp.Client(
        username=request_json['secrets']['MYFITNESSPAL_USERNAME'],
        password=request_json['secrets']['MYFITNESSPAL_PASSWORD'])

    # initialize state for the case when fivetran is starting from scratch.
    # put initial values for the cursor and tokens in the 'secrets' node.
    # fivetran should automatically keep track of subsequent updates in the
    # 'state' node.
    if 'cursor' not in request_json['state']:
        request_json['state']['cursor'] = request_json['secrets']['cursor']

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
            'state': {
                'cursor': cursor,
            },
            'hasMore': False
        }

    # otherwise the cursor must be in the past so go ahead and pull data.

    # pull data from the two most recent days to catch cases when I forget to
    # enter data for dinner until the next morning
    prev_date = cursor_date - dt.timedelta(days=1)

    total_records = [
        {'date': date.isoformat(), 'name': meal.name, **meal.totals}
        for date in [prev_date, cursor_date]
        for meal in client.get_date(date.year, date.month, date.day).meals
        ]

    return {
        'state': {
            'cursor': (cursor_date + dt.timedelta(days=1)).isoformat()
        },
        'insert': {
            'totals': total_records
        },
        'delete': {
        },
        'schema': {
            'totals': {
                'primary_key': ['date', 'name']
            }
        },
        'hasMore': True
    }
