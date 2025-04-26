import base64
from typing import TYPE_CHECKING, Union, List, Tuple
import json
from crc32c import crc32c
from rm_api.notifications.models import APIFatal
from rm_api.storage.v3 import make_storage_request, get_documents_using_root, check_file_exists

if TYPE_CHECKING:
    from rm_api import API
    from rm_api.models import *

SYNC_ROOT_URL = "{0}sync/v3/root"


def get_root(api: 'API'):
    if api.offline_mode:
        return {"hash": api.last_root}
    return make_storage_request(api, "GET", SYNC_ROOT_URL) or {}


class RootUploadFailure(Exception):
    """This happens if it was updated by another process"""

    def __init__(self):
        super().__init__("Failed to update root")


def update_root(api: 'API', root: dict):
    data = json.dumps(root, indent=4).encode('utf-8')
    checksum_bs4 = base64.b64encode(crc32c(data).to_bytes(4, 'big')).decode('utf-8')
    exists = check_file_exists(api, root['hash'], use_cache=False)
    if not exists and not api.ignore_error_protection:
        api.spread_event(APIFatal())
        raise Exception("The root file attempted to be set was not on the server")
    response = api.session.put(
        SYNC_ROOT_URL.format(api.document_storage_uri),
        data=data,
        headers={
            **api.session.headers,
            'rm-filename': 'roothash',
            'Content-Type': 'application/json',
            'x-goog-hash': f'crc32c={checksum_bs4}',
        },
    )

    if not response.ok:
        raise RootUploadFailure()
    else:
        api.log("Root updated:", response.json())
    return True


def get_documents_old_sync(api: 'API', progress):
    root = get_root(api).get('hash', 'miss')
    api.last_root = root
    return get_documents_using_root(api, progress, root)
