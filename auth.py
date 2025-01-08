from typing import TYPE_CHECKING
from uuid import uuid4

import requests

if TYPE_CHECKING:
    from rm_api import API

TOKEN_URL = "{0}token/json/2/device/new"
TOKEN_REFRESH_URL = "{0}token/json/2/user/new"


class FailedToRefreshToken(Exception):
    pass


class FailedToGetToken(Exception):
    pass


def get_token(api: 'API', code: str = None):
    if not api.require_token and not code:
        return None
    if not code:
        code = input("Enter your connect code: ")
    response = api.session.post(
        TOKEN_URL.format(api.uri),
        json={
            "code": code,
            "deviceDesc": "desktop-windows",
            "deviceID": uuid4().hex,
            "secret": ""
        },
        headers={
            "Authorization": f"Bearer "
        }
    )
    if response.status_code != 200:
        if api.require_token:
            print(f'Got status code {response.status_code}')
            return get_token(api)
        else:
            raise FailedToGetToken("Could not get token")

    with open(api.token_file_path, "w") as f:
        f.write(response.text)

    return response.text


def refresh_token(api: 'API', token: str):
    if not token:
        if api.require_token:
            return refresh_token(api, get_token(api))
    try:
        response = requests.post(
            TOKEN_REFRESH_URL.format(api.uri),
            headers={"Authorization": f"Bearer {token}"},
            timeout=1,
        )
    except (TimeoutError, requests.exceptions.ConnectionError):
        api.offline_mode = True
        return None
    if response.status_code != 200:
        if api.require_token:
            return refresh_token(api, get_token(api))
        else:
            raise FailedToRefreshToken("Could not refresh token")

    return response.text
