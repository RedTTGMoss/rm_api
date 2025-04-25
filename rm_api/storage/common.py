from hashlib import sha256
import os
from io import IOBase
from typing import TYPE_CHECKING

from crc32c import crc32c

DOCUMENT_STORAGE_URL = "{0}service/json/1/document-storage?environment=production&group=auth0%7C5a68dc51cb30df3877a1d7c4&apiVer=2"
DOCUMENT_NOTIFICATIONS_URL = "{0}service/json/1/notifications?environment=production&group=auth0%7C5a68dc51cb30df3877a1d7c4&apiVer=1"

if TYPE_CHECKING:
    from rm_api import API, FileSyncProgress, DocumentSyncProgress


def get_document_storage_uri(api: 'API'):
    response = api.session.get(DOCUMENT_STORAGE_URL.format(api.discovery_uri))
    host = response.json().get("Host")
    secure = 'https'
    if host == 'local.appspot.com':
        secure, root_host = api.uri.split('://')
        root_host = root_host.split("/")[0]
    else:
        root_host = host
    root_response = api.session.get(f"{secure}://{root_host}/sync/v3/root")
    if root_response.status_code == 400:
        api.use_new_sync = True
        return None
    return host


def get_document_notifications_uri(api: 'API'):
    response = api.session.get(DOCUMENT_NOTIFICATIONS_URL.format(api.discovery_uri))
    host = response.json().get("Host")
    if host == 'local.appspot.com':  # rM Fake Cloud by DDVK
        host = api.uri.split("://")[1].split("/")[0]
    return host


class FileHandle:
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_handle = None
        file_stats = os.stat(file_path)
        self.file_size = file_stats.st_size
        self._hash = None
        self.checksum = 0

    def open(self):
        if not self.file_handle:
            self.file_handle = open(self.file_path, 'rb')

    def read(self, size=None):
        self.open()
        return self.file_handle.read(size)

    def readinto(self, b):
        self.open()
        return self.file_handle.readinto(b)

    def reset(self):
        if self.file_handle:
            self.file_handle.seek(0)

    def seek(self, offset, whence=0):
        self.open()
        return self.file_handle.seek(offset, whence)

    def tell(self):
        self.open()
        return self.file_handle.tell()

    def close(self):
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None

    def __len__(self):
        return self.file_size

    def hash(self):
        if self._hash:
            return self._hash
        hasher = sha256()
        self.checksum = 0
        self.open()
        while True:
            data = self.file_handle.read(4096)
            if not data:
                break
            hasher.update(data)
            self.checksum = crc32c(data, self.checksum)
        self._hash = hasher.hexdigest()
        return self._hash

    def crc32c(self):
        if self.checksum:
            return self.checksum
        else:
            self.hash()
            return self.checksum

    def __copy__(self):
        self.close()
        obj = self.__class__(self.file_path)
        obj._hash = self._hash
        obj.checksum = self.checksum

    def __deepcopy__(self, memo: dict = None):
        return self.__copy__()


class ProgressFileAdapter(IOBase):
    def __init__(self, document_sync: 'DocumentSyncProgress', file_sync: 'FileSyncProgress', data: FileHandle):
        self.document_sync = document_sync
        self.file_sync = file_sync
        self.data = data
        if isinstance(data, FileHandle):
            data.reset()

    def read(self, size=-1):
        if size < 0:
            raise ValueError("The size argument is required and must be non-negative.")

        index = self.file_sync.done
        if isinstance(self.data, FileHandle):
            chunk = self.data.read(size)
        elif isinstance(self.data, bytes):
            chunk = self.data[index:index + size]
        self.file_sync.done += len(chunk)
        self.document_sync.done += len(chunk)
        if self.file_sync.finished and isinstance(self.data, FileHandle):
            self.data.close()
        return chunk

    def reset(self):
        self.document_sync.done -= self.file_sync.done
        self.file_sync.done = 0
        if isinstance(self.data, FileHandle):
            self.data.reset()

    def __len__(self):
        return self.file_sync.total
