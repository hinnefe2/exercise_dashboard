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

    client = mfp.Client(username=request_json['secrets']['MYFITNESSPAL_USERNAME'],
                        password=request_json['secrets']['MYFITNESSPAL_PASSWORD'])

    # start at the beginning of 2017 if no starting date given
    if 'cursor' not in request_json['state']:
        cursor = '2017-01-01T00:00:00'
    else:
        cursor = request_json['state']['cursor']

    day = parser.parse(cursor)
    next_day = day + dt.timedelta(days=1)

    diary = client.get_date(day.year, day.month, day.day)

    total_records = [
        {'date': day.isoformat(), 'name': meal.name, **meal.totals}
        for meal in diary.meals
        ]

    has_more = day < dt.datetime.today()

    response = {
        'state': {
            'cursor': next_day.isoformat()
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
        'hasMore': has_more
    }

    return response
