"""
This script generates an OAuth2 refresh token for the fitbit API using the
Authorization Code Grant Flow:
https://dev.fitbit.com/build/reference/web-api/oauth2/#authorization-code-grant-flow

Use this script to generate an initial accesss and refresh token pair.
"""

import base64
import os
import pprint

import requests

from urllib.parse import urlparse, parse_qs


SCOPES = 'activity%20weight'

# must be included in the list of callback URLs in the fitbit app config page
# https://dev.fitbit.com/apps/details/<client id>
REDIRECT_URI = 'https://127.0.0.1:8080/'


def main():

    if not ('FITBIT_CLIENT_ID' in os.environ) and ('FITBIT_CLIENT_SECRET' in os.environ):
        raise ValueError(
            'Must have Fitbit app credentials in environment variables. See '
            'https://dev.fitbit.com/build/reference/web-api/basics/#app-registration '
            'for how to register a Fitbit app and get credentials.')

    authorization_url = (
        f'https://www.fitbit.com/oauth2/authorize?'
        f'response_type=code&'
        f'client_id={os.getenv("FITBIT_CLIENT_ID")}&'
        f'redirect_uri={REDIRECT_URI}&'
        f'scope={SCOPES}')

    print("Go to\n"
          f"{authorization_url}\n"
          "click 'Allow', then copy the entire redirected URL")

    auth_response_uri = input('Enter authorization redirected URL: ')

    # pull the 'code' and 'state' out of the callback response URL
    code_state = parse_qs(urlparse(auth_response_uri).query)

    # I always run into trouble using the requests-oauthlib library (especially
    # https://github.com/requests/requests-oauthlib/issues/324) so let's just
    # use the vanilla requests library and build our own headers
    id_secret = (f'{os.getenv("FITBIT_CLIENT_ID")}:'
                 f'{os.getenv("FITBIT_CLIENT_SECRET")}')

    # see https://dev.fitbit.com/build/reference/web-api/oauth2/#refreshing-tokens  # noqa
    # for why we have to do this base64 stuff
    b64_creds = (base64.encodebytes(bytes(id_secret, 'utf8'))
                       .decode('utf8')
                       .rstrip())

    auth_header = {'Authorization': f'Basic {b64_creds}',
                   'Content-Type': 'application/x-www-form-urlencoded'}

    post_params = {'grant_type': 'authorization_code',
                   'redirect_uri': REDIRECT_URI,
                   **code_state}

    resp = requests.post('https://api.fitbit.com/oauth2/token',
                         headers=auth_header, params=post_params)

    token = resp.json()

    pprint.pprint(token)


if __name__ == '__main__':
    main()
