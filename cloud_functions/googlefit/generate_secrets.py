"""
This script generates an OAuth2 refresh token for the Google Fit API.

Use this script to generate a 'secrets' JSON blob for the Fivetran connector.
"""

import argparse
import json

import requests

from urllib.parse import urlparse, parse_qs


SCOPES = 'https://www.googleapis.com/auth/fitness.activity.read'
REDIRECT_URI = 'https://127.0.0.1:8080/'


def main(id_file, cursor):

    with open(id_file) as infile:
        config = json.load(infile)['installed']

    authorization_url = (
        f'{config["auth_uri"]}?'
        f'response_type=code&'
        f'client_id={config["client_id"]}&'
        f'access_type=offline&'
        f'prompt=consent&'
        f'flowName=GeneralOAuthFlow&'
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
    header = {'Content-Type': 'application/x-www-form-urlencoded'}

    post_params = {'grant_type': 'authorization_code',
                   'redirect_uri': REDIRECT_URI,
                   'client_id': config['client_id'],
                   'client_secret': config['client_secret'],
                   **code_state}

    resp = requests.post(config['token_uri'],
                         headers=header, params=post_params)

    token = resp.json()

    secrets = {
        'GFIT_CLIENT_ID': config['client_id'],
        'GFIT_CLIENT_SECRET': config['client_secret'],
        'refresh_token': token['refresh_token'],
        'access_token': token['access_token'],
        'cursor': cursor}

    print(json.dumps(secrets, indent=0))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('id_file', help='client id JSON file')
    parser.add_argument('cursor', help='date to start scraping from')

    args = parser.parse_args()

    main(args.id_file, args.cursor)
