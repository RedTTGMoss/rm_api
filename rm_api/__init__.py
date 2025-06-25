import asyncio
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from traceback import print_exc
from typing import Dict, List, Union

import colorama
import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from .auth import MissingTabletLink, get_token, refresh_token
from .download_lock import DownloadLock
from .models import DocumentCollection, Document, Metadata, Content, make_uuid, File, make_hash
from .notifications import handle_notifications
from .notifications.models import FileSyncProgress, SyncRefresh, DocumentSyncProgress, NewDocuments, APIFatal, \
    DownloadOperation
from .storage.common import get_document_storage_uri, get_document_notifications_uri
from .storage.exceptions import NewSyncRequired
from .storage.new_sync import get_documents_new_sync, handle_new_api_steps
from .storage.new_sync import get_root as get_root_new
from .storage.old_sync import get_documents_old_sync, update_root, RootUploadFailure
from .storage.old_sync import get_root as get_root_old
from .storage.v3 import get_documents_using_root, get_file, get_file_contents, make_files_request, put_file, \
    check_file_exists
from .sync_stages import *

colorama.init()

DEFAULT_REMARKABLE_URI = "https://webapp.cloud.remarkable.com/"
DEFAULT_REMARKABLE_DISCOVERY_URI = "https://service-manager-production-dot-remarkable-production.appspot.com/"


def retry_on_version_bump(fn):
    """
    Just retries the function if NewSyncRequired is raised.
    The API should have already been updated to use the new sync version,
    so this is just a way to handle the case where the API version has changed and relay to retry the operation.
    """

    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        try:
            return fn(self, *args, **kwargs)
        except NewSyncRequired:
            return fn(self, *args, **kwargs)

    return wrapper


class API:
    document_collections: Dict[str, DocumentCollection]
    documents: Dict[str, Document]

    def __init__(self, require_token: bool = True, token_file_path: str = 'token', sync_file_path: str = 'sync',
                 uri: str = None, discovery_uri: str = None, author_id: str = None, log_file='rm_api.log'):
        self.retry_strategy = Retry(
            total=10,
            backoff_factor=2,
            status_forcelist=(429, 503)
        )
        http_adapter = HTTPAdapter(max_retries=self.retry_strategy)
        self.session = requests.Session()
        self.session.mount("http://", http_adapter)
        self.session.mount("https://", http_adapter)

        self.token_file_path = token_file_path
        if not author_id:
            self.author_id = make_uuid()
        else:
            self.author_id = author_id
        self.uri = uri or os.environ.get("URI", DEFAULT_REMARKABLE_URI)
        self.discovery_uri = discovery_uri or os.environ.get("DISCOVERY_URI", DEFAULT_REMARKABLE_DISCOVERY_URI)
        self.sync_file_path = sync_file_path
        if self.sync_file_path is not None:
            os.makedirs(self.sync_file_path, exist_ok=True)
        self.last_root = None
        self.offline_mode = False
        self.document_storage_uri = None
        self.document_notifications_uri = None
        self._upload_lock = threading.Lock()
        self._hook_lock = threading.Lock()
        self.download_lock = DownloadLock()
        self.sync_notifiers: int = 0
        self.download_operations = set()
        self._hook_list = {}  # Used for event hooks
        self._use_new_sync = False
        # noinspection PyTypeChecker
        self.document_collections = {}
        # noinspection PyTypeChecker
        self.documents = {}
        self._token = None
        self.debug = False
        self.ignore_error_protection = False
        self.connected_to_notifications = False
        self.require_token = require_token
        if not self.uri.endswith("/"):
            self.uri += "/"
        if not self.discovery_uri.endswith("/"):
            self.discovery_uri += "/"
        token = os.environ.get("TOKEN")
        if token is None:
            if os.path.exists(self.token_file_path):
                token = open(self.token_file_path).read()
                try:
                    self.set_token(token)
                except MissingTabletLink:
                    self.set_token(token, remarkable=True)
            else:
                self.get_token()
        else:
            self.token = token

        self.log_file = log_file
        self.log_lock = threading.Lock()

        # Set up logging configuration
        logging.basicConfig(filename=self.log_file, level=logging.INFO,
                            format='%(asctime)s - %(message)s',
                            filemode='a')  # 'a' for append mode
        self.loop = asyncio.get_event_loop()

    @property
    def hook_list(self):
        return self._hook_list

    @property
    def online_download_operations(self):
        return [op for op in list(self.download_operations) if op.stage in (
            DOWNLOAD_CONTENT,
            GET_CONTENTS
        )]

    def force_stop_all(self):
        for operation in list(self.download_operations):
            self.cancel_download_operation(operation, reason='force stop')

    def add_download_operation(self, operation: DownloadOperation):
        if not isinstance(operation, DownloadOperation):
            raise TypeError("operation must be an instance of DownloadOperation")
        self.download_operations.add(operation)

    def remove_download_operation(self, operation: DownloadOperation):
        if not isinstance(operation, DownloadOperation):
            raise TypeError("operation must be an instance of DownloadOperation")
        try:
            self.download_operations.remove(operation)
        except KeyError:
            pass

    def begin_download_operation(self, operation: DownloadOperation):
        self.add_download_operation(operation)
        self.spread_event(operation.begin_event)

    def poll_download_operation(self, operation: DownloadOperation):
        self.add_download_operation(operation)
        self.spread_event(operation.poll_event)

    def finish_download_operation(self, operation: DownloadOperation):
        self.remove_download_operation(operation)
        operation.finish()
        self.spread_event(operation.finish_event)

    def cancel_download_operation(self, operation: DownloadOperation, reason: str = 'canceled'):
        self.remove_download_operation(operation)
        operation.cancel(reason)
        self.spread_event(operation.cancel_event)

    @property
    def downloading(self):
        if len(self.download_operations) == 0:
            return False
        return any(not op.finished for op in self.online_download_operations if not op.canceled)

    @property
    def download_done(self):
        return sum(op.done for op in self.online_download_operations if not op.canceled)

    @property
    def download_total(self):
        return sum(op.total for op in self.online_download_operations if not op.canceled)

    def reconnect(self):
        self.connected_to_notifications = False
        self._use_new_sync = False
        self.offline_mode = False
        self.document_storage_uri = None
        self.document_notifications_uri = None
        if not self.uri.endswith("/"):
            self.uri += "/"
        if not self.discovery_uri.endswith("/"):
            self.discovery_uri += "/"

    @property
    def use_new_sync(self):
        return self._use_new_sync

    def connect_to_notifications(self):
        if self.connected_to_notifications or self.offline_mode:
            return
        self.check_for_document_notifications()
        handle_notifications(self)
        self.connected_to_notifications = True

    @use_new_sync.setter
    def use_new_sync(self, value):
        if not self._use_new_sync and value:
            handle_new_api_steps(self)
        self._use_new_sync = value

    @property
    def token(self):
        return self._token

    def set_token(self, value, remarkable: bool = False):
        if not value:
            return
        token = refresh_token(self, value, remarkable)
        self.session.headers["Authorization"] = f"Bearer {token}"
        self._token = token

    @token.setter
    def token(self, value):
        self.set_token(value)

    def get_token(self, code: str = None, remarkable: bool = False):
        self.set_token(get_token(self, code, remarkable), remarkable)

    @retry_on_version_bump
    def get_documents(self, progress=lambda d, i: None):
        self.check_for_document_storage()
        if self.use_new_sync:
            get_documents_new_sync(self, progress)
        else:
            get_documents_old_sync(self, progress)

    @retry_on_version_bump
    def get_root(self):
        self.check_for_document_storage()
        if self.use_new_sync:
            return get_root_new(self)
        else:
            return get_root_old(self)

    def spread_event(self, event: object):
        with self._hook_lock:
            for hook in self._hook_list.values():
                hook(event)

    def add_hook(self, hook_id, hook):
        with self._hook_lock:
            self._hook_list[hook_id] = hook

    def remove_hook(self, hook_id):
        with self._hook_lock:
            try:
                del self._hook_list[hook_id]
            except KeyError:
                pass

    def check_for_document_storage(self):
        if self.offline_mode:
            return
        if not self.document_storage_uri:
            uri = get_document_storage_uri(self)
            if not uri:
                return
            elif uri == 'local.appspot.com':
                uri = self.uri
            else:
                if not uri.endswith("/"):
                    uri += "/"
                uri = f'https://{uri}'

            self.document_storage_uri = uri

    def upload(self, document: Union[Document, DocumentCollection], callback=None, unload: bool = False):
        self.upload_many_documents([document], callback, unload)

    def upload_many_documents(self, documents: List[Union[Document, DocumentCollection]], callback=None,
                              unload: bool = False):
        self.sync_notifiers += 1
        self._upload_lock.acquire()
        upload_event = FileSyncProgress()
        self.spread_event(upload_event)
        upload_event.stage = STAGE_START
        try:
            # for document in documents:
            #     document.ensure_download()
            self._upload_document_contents(documents, upload_event)
        except:
            print_exc()
        finally:
            upload_event.finished = True
            if unload:
                for document in documents:
                    document.unload_files()
            time.sleep(.1)
            self._upload_lock.release()
            self.sync_notifiers -= 1

    def delete(self, document: Union[Document, DocumentCollection], callback=None, unload: bool = True):
        self.delete_many_documents([document], callback, unload)

    def delete_many_documents(self, documents: List[Union[Document, DocumentCollection]], callback=None,
                              unload: bool = True):
        self.sync_notifiers += 1
        self._upload_lock.acquire()
        upload_event = FileSyncProgress()
        self.spread_event(upload_event)
        upload_event.stage = STAGE_START
        try:
            self._delete_document_contents(documents, upload_event)
        except:
            print_exc()
        finally:
            upload_event.finished = True
            if unload:
                for document in documents:
                    document.unload_files()
            time.sleep(.1)
            self._upload_lock.release()
            self.sync_notifiers -= 1

    def _upload_document_contents(self, documents: List[Union[Document, DocumentCollection]],
                                  progress: FileSyncProgress):
        # We need to upload the content, metadata, rm file, file list and update root
        # This is the order that remarkable expects the upload to happen in, anything else and they might detect it as
        # API tampering, so we want to follow their upload cycle
        if self.offline_mode:
            progress.total = 1
            progress.done = 1
            return

        progress.total += 2  # Getting root / Updating root

        progress.stage = STAGE_GET_ROOT

        root = self.get_root()  # root info

        _, files = get_file(self, root['hash'])
        progress.done += 1  # Got root

        new_root = {
            "broadcast": True,
            "generation": root['generation']
        }

        document_files = [
            File(
                None,
                document.uuid,
                len(document.files), 0,
                f"{document.uuid}.docSchema"
            )
            for document in documents
        ]

        uuids = [document.uuid for document in documents]
        new_root_files = document_files + [
            file
            for file in files
            if file.uuid not in uuids
        ]

        old_files = []
        files_with_changes = []

        progress.stage = STAGE_EXPORT_DOCUMENTS
        for document in documents:
            document.check()
            document.export()
            document.provision = True
            progress.total += len(document.files)
        self.documents.update({
            document.uuid: document
            for document in documents if isinstance(document, Document)
        })
        self.document_collections.update({
            document_collection.uuid: document_collection
            for document_collection in documents if isinstance(document_collection, DocumentCollection)
        })
        self.spread_event(NewDocuments())

        # Figure out what files have changed
        progress.stage = STAGE_DIFF_CHECK_DOCUMENTS
        for document in documents:
            for file in document.files:
                try:
                    exists = check_file_exists(self, file.hash, binary=True, use_cache=False)
                    if not exists:
                        files_with_changes.append(file)
                    else:
                        old_files.append(file)
                except:
                    files_with_changes.append(file)
                finally:
                    progress.done += 1

        progress.stage = STAGE_PREPARE_DATA

        # Copy the content data so we can add more files to it
        content_datas = {}
        for document in documents:
            content_datas.update(document.content_data.copy())

        # Update the hash for files that have changed
        for file in files_with_changes:
            if data := content_datas.get(file.uuid):
                file.hash = make_hash(data)
                file.size = len(data)

        # Make a new document file with the updated files for this document

        progress.stage = STAGE_COMPILE_DATA

        for document, document_file in zip(documents, document_files):
            document_file_content = document_file.update_document_file(self, document.files, content_datas)

            # Add the document file to the content_data
            content_datas[document_file.uuid] = document_file_content
            files_with_changes.append(document_file)

        # Prepare the root file
        progress.stage = STAGE_PREPARE_ROOT
        root_file_content, root_file = File.create_root_file(new_root_files)
        new_root['hash'] = root_file.hash

        files_with_changes.append(root_file)
        content_datas[root_file.uuid] = root_file_content

        # Upload all the files that have changed
        document_operations = {}
        progress.stage = STAGE_PREPARE_OPERATIONS

        for document in documents:
            document_sync_operation = DocumentSyncProgress(document.uuid, progress)
            document_operations[document.uuid] = document_sync_operation

        progress.stage = STAGE_UPLOAD

        futures = []
        progress.total += len(files_with_changes)
        with ThreadPoolExecutor(max_workers=4) as executor:
            loop = asyncio.new_event_loop()  # Get the current event loop
            for file in sorted(files_with_changes, key=lambda f: f.size):
                if (document_uuid := file.uuid.split('/')[0].split('.')[0]) in document_operations:
                    document_operation = document_operations[document_uuid]
                else:
                    document_operations[file.uuid] = DocumentSyncProgress(file.uuid, progress)
                    document_operation = document_operations[file.uuid]

                if file.uuid.endswith('.content') or file.uuid.endswith('.metadata'):
                    file.save_to_cache(self, content_datas[file.uuid])

                # This is where you use run_in_executor to call your async function in a separate thread
                future = loop.run_in_executor(executor, put_file, self, file, content_datas[file.uuid],
                                              document_operation)
                futures.append(future)
            executor.shutdown(wait=True)

        # Wait for operation to finish
        while not all(operation.finished for operation in document_operations.values()):
            time.sleep(.1)

        # Update the root
        progress.stage = STAGE_UPDATE_ROOT
        try:
            update_root(self, new_root)
        except RootUploadFailure:
            self.log("Sync root failed, this is fine if you decided to sync on another device / start a secondary sync")
            progress.done = -1
            progress.total = 0
            self._upload_document_contents(documents, progress)
        progress.done += 1  # Update done finally matching done/total

        for document in documents:
            document.content_data.clear()
            document.files_available = document.check_files_availability()
            document.provision = False

        if self.sync_notifiers <= 1:
            self.spread_event(SyncRefresh())

    def _delete_document_contents(self, documents: List[Union[Document, DocumentCollection]],
                                  progress: FileSyncProgress):
        # We need to remove the documents from the root and upload the new root

        if self.offline_mode:
            progress.total = 1
            progress.done = 1
            return

        progress.total += 2  # Getting root / Updating root

        progress.stage = STAGE_GET_ROOT

        root = self.get_root()  # root info

        _, files = get_file(self, root['hash'])
        progress.done += 1  # Got root

        new_root = {
            "broadcast": True,
            "generation": root['generation']
        }

        uuids = [document.uuid for document in documents]

        new_root_files = [
            file
            for file in files
            if file.uuid not in uuids
        ]  # Include all the old data without this data

        # Prepare the root file
        progress.stage = STAGE_PREPARE_ROOT
        root_sync_operation = DocumentSyncProgress('root', progress)
        root_file_content, root_file = File.create_root_file(new_root_files)
        new_root['hash'] = root_file.hash

        put_file(self, root_file, root_file_content, root_sync_operation)

        # Update the root
        progress.stage = STAGE_UPDATE_ROOT
        try:
            update_root(self, new_root)
        except RootUploadFailure:
            self.log("Sync root failed, this is fine if you decided to sync on another device / start a secondary sync")
            progress.done = -1
            progress.total = 0
            self._upload_document_contents(documents, progress)
        progress.done += 1  # Update done finally matching done/total

        if self.sync_notifiers <= 1:
            self.spread_event(SyncRefresh())

    def check_for_document_notifications(self):
        if not self.document_notifications_uri:
            uri = get_document_notifications_uri(self)
            if not uri:
                return
            elif uri == 'local.appspot.com':
                uri = self.uri
            else:
                if not uri.endswith("/"):
                    uri += "/"
                uri = f'https://{uri}'
            self.document_notifications_uri = uri

    def log(self, *args, enable_print=False):
        with self.log_lock:
            if self.debug and enable_print:
                print(*args)
            logging.info(' '.join(map(str, args)))

    def reset_root(self):
        root = self.get_root()

        new_root = {
            "broadcast": True,
            "generation": root.get('generation', 0)
        }

        root_file_content = b'3\n'

        root_file = models.File(models.make_hash(root_file_content), f"root.docSchema", 0, len(root_file_content))
        new_root['hash'] = root_file.hash
        put_file(self, root_file, root_file_content, DocumentSyncProgress(''))
        update_root(self, new_root)
        _, files = get_file(self, new_root['hash'])
