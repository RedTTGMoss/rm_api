import asyncio
import base64
import json
import os
import ssl
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from hashlib import sha256
from json import JSONDecodeError
from traceback import format_exc, print_exc
from typing import TYPE_CHECKING, Union, Tuple, List, Set

import aiohttp
import certifi
from aiohttp import ClientTimeout
from colorama import Fore, Style
from crc32c import crc32c
from urllib3 import Retry

import rm_api.models as models
from rm_api.defaults import DocumentTypes
from rm_api.helpers import batched, download_operation_wrapper, download_operation_wrapper_with_stage
from rm_api.notifications.models import APIFatal, DownloadOperation
from rm_api.notifications.models import DocumentSyncProgress, FileSyncProgress
from rm_api.storage.common import FileHandle, ProgressFileAdapter
from rm_api.storage.exceptions import NewSyncRequired
from rm_api.sync_stages import FETCH_FILE, GET_CONTENTS, GET_FILE, LOAD_CONTENT, MISSING_CONTENT, FETCH_CACHE

FILES_URL = "{0}sync/v3/files/{1}"

ssl_context = ssl.create_default_context(cafile=certifi.where() if os.name == 'darwin' else None)

if TYPE_CHECKING:
    from rm_api import API
    from rm_api.models import File

DEFAULT_ENCODING = 'utf-8'
EXTENSION_ORDER = ['content', 'metadata', 'rm']


# if os.name == 'nt':
#     asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

class CacheMiss(Exception):
    pass


def get_file_item_order(item: 'File'):
    try:
        return EXTENSION_ORDER.index(item.uuid.rsplit('.', 1)[-1])
    except ValueError:
        return -1


def make_storage_request(api: 'API', method, request, data: dict = None) -> Union[str, None, dict]:
    response = api.session.request(
        method,
        request.format(api.document_storage_uri),
        json=data or {},
    )

    if response.status_code == 400:
        api.use_new_sync = True
        raise NewSyncRequired()
    if response.status_code != 200:
        return None
    try:
        return response.json()
    except JSONDecodeError:
        return response.text


def make_files_request(api: 'API', method, file, data: dict = None, binary: bool = False, use_cache: bool = True,
                       enforce_cache: bool = False, operation: DownloadOperation = None) -> \
        Union[str, None, dict, bool, bytes]:
    if method == 'HEAD':
        method = 'GET'
        head = True
    else:
        head = False
    if api.sync_file_path:
        location = os.path.join(api.sync_file_path, file)
    else:
        location = None
    if use_cache and location and os.path.exists(location):
        operation.total = os.path.getsize(location)
        if head:
            operation.stage = FETCH_CACHE
            api.poll_download_operation(operation)
            return True
        operation.stage = LOAD_CONTENT
        api.begin_download_operation(operation)
        if binary:
            with open(location, 'rb') as f:
                cache_data = f.read()
        else:
            with open(location, 'r', encoding=DEFAULT_ENCODING) as f:
                data = f.read()
            try:
                cache_data = json.loads(data)
            except JSONDecodeError:
                cache_data = data
        operation.done = operation.total
        return cache_data
    if enforce_cache:
        # Checked cache and it wasn't there
        raise CacheMiss()

    response = api.session.request(
        method,
        FILES_URL.format(api.document_storage_uri, file),
        json=data or None,
        stream=True,
        allow_redirects=not head
    )
    operation.use_response(response, head)
    if head:
        api.poll_download_operation(operation)
    else:
        api.begin_download_operation(operation)

    if head and response.status_code in (302, 404, 200):
        response.close()  # We don't need the body for HEAD requests
        return response.status_code != 404
    elif head:
        operation.use_response(response)

    if operation.first_chunk == b'{"message":"invalid hash"}\n':
        response.close()
        operation.stage = MISSING_CONTENT
        return None
    elif not response.ok:
        response.close()
        operation.stage = MISSING_CONTENT
        raise Exception(f"Failed to make files request - {response.status_code}\n{response.text}")

    if location:
        try:
            with open(location, "wb") as f:
                f.write(operation.first_chunk)
                for chunk in operation.iter():
                    f.write(chunk)
        except DownloadOperation.DownloadCancelException as e:
            if os.path.exists(location):
                os.remove(location)
            raise e

    if binary:
        return operation.get_bytes()
    else:
        text = operation.get_text()
        try:
            return json.loads(text)
        except JSONDecodeError:
            return text


async def fetch_with_retries(session: aiohttp.ClientSession, url: str, method: str, retry_strategy: Retry,
                             data_adapter: ProgressFileAdapter,
                             **kwargs):
    attempt = 0
    retries = retry_strategy.total
    backoff_factor = retry_strategy.backoff_factor
    retry_statuses = retry_strategy.status_forcelist

    while attempt < retries:
        try:
            async with session.request(
                    method, url,
                    data=data_adapter,
                    **kwargs
            ) as response:
                await response.read()
                if response.status in retry_statuses:
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"HTTP error with status code {response.status}"
                    )
                else:
                    await response.read()
            return response
        except (aiohttp.ClientResponseError, asyncio.TimeoutError) as e:
            attempt += 1
            if attempt < retries:
                wait_time = backoff_factor * (2 ** (attempt - 1))
                data_adapter.reset()
                await asyncio.sleep(wait_time)
            else:
                raise e
    return None


async def put_file_async(api: 'API', file: 'File', data: bytes, sync_event: DocumentSyncProgress):
    if isinstance(data, FileHandle):
        crc_result = data.crc32c()
    else:
        crc_result = crc32c(data)
    checksum_bs4 = base64.b64encode(crc_result.to_bytes(4, 'big')).decode('utf-8')
    content_length = len(data)

    upload_progress = FileSyncProgress()
    upload_progress.total = content_length

    sync_event.total += content_length
    sync_event.add_task()

    data_adapter = ProgressFileAdapter(sync_event, upload_progress, data)
    timeout = ClientTimeout(total=3600)  # One hour timeout limit
    google = False

    async with aiohttp.ClientSession(timeout=timeout,
                                     connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        # Try uploading through remarkable
        try:
            response = await fetch_with_retries(
                session,
                FILES_URL.format(api.document_storage_uri, file.hash),
                'PUT',
                api.retry_strategy,
                data_adapter,
                headers=(headers := {
                    **api.session.headers,
                    'content-length': str(content_length),
                    'content-type': 'application/octet-stream',
                    'rm-filename': file.rm_filename,
                    'x-goog-hash': f'crc32c={checksum_bs4}'
                }),
                allow_redirects=False
            )
        except:
            api.log(format_exc())
            return False

        if response.status == 302:
            google = True
            # Reset progress, start uploading through google instead
            data_adapter.reset()

            try:
                api.log("Google signed url was provided by the API, uploading to that now.")
                response = await fetch_with_retries(
                    session,
                    response.headers['location'],
                    'PUT',
                    api.retry_strategy,
                    data_adapter,
                    headers={
                        **headers,
                        'x-goog-content-length-range': response.headers['x-goog-content-length-range'],
                        'x-goog-hash': f'crc32c={checksum_bs4}'
                    }
                )
            except:
                api.log(format_exc())
                return False
    if response.status == 400:
        if '<Code>ExpiredToken</Code>' in await response.text():
            data_adapter.reset()
            sync_event.finish_task()
            # Try again
            api.log("Put file timed out, this is okay, trying again...")
            return await put_file_async(api, file, data, sync_event)

    if response.status > 299 or response.status < 200:
        api.log(f"Put file failed google: {google} -> {response.status}\n{await response.text()}")
        return False
    else:
        api.log(file.uuid, "uploaded")

    sync_event.finish_task()
    return True


def put_file(api: 'API', file: 'File', data: bytes, sync_event: DocumentSyncProgress):
    api.spread_event(sync_event)
    loop = asyncio.new_event_loop()

    try:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(put_file_async(api, file, data, sync_event))
    except:
        print_exc()
    finally:
        loop.close()


@download_operation_wrapper_with_stage(GET_FILE)
def get_file(api: 'API', file, use_cache: bool = True, raw: bool = False, operation: DownloadOperation = None) -> Tuple[
    int, Union[List['File'], List[str]]]:
    res = make_files_request(api, "GET", file, use_cache=use_cache, operation=operation)
    if not res:
        return -1, []
    if isinstance(res, int):
        return res, []
    version, *lines = res.splitlines()
    if raw:
        return version, lines
    return version, [models.File.from_line(line) for line in lines]


@download_operation_wrapper_with_stage(GET_CONTENTS)
def get_file_contents(api: 'API', file, binary: bool = False, use_cache: bool = True, enforce_cache: bool = False,
                      operation: DownloadOperation = None):
    return make_files_request(api, "GET", file, binary=binary, use_cache=use_cache, enforce_cache=enforce_cache,
                              operation=operation)


@download_operation_wrapper
def _check_file_exists(api: 'API', file, binary: bool = False, use_cache: bool = True,
                       operation: DownloadOperation = None):
    return make_files_request(api, "HEAD", file, binary=binary, use_cache=use_cache, operation=operation)


@lru_cache(maxsize=600)
def check_file_exists(api: 'API', file, binary: bool = False, use_cache: bool = True,
                      operation: DownloadOperation = None):
    return _check_file_exists(api, file, binary=binary, use_cache=use_cache, ref=file, stage=FETCH_FILE,
                              operation=operation)


@lru_cache(maxsize=600)
def poll_file(api: 'API', file, binary: bool = False, use_cache: bool = True,
              operation: DownloadOperation = None):
    return _check_file_exists(api, file, binary=binary, use_cache=use_cache, ref=file, stage=FETCH_FILE,
                              operation=operation)


def process_file_content(
        file_content: List['File'],
        file: 'File',
        deleted_document_collections_list: Set,
        deleted_documents_list: Set,
        document_collections_with_items: Set,
        badly_hashed: List,
        api: 'API',
        matches_hash: bool
):
    content = None

    for item in file_content:
        if item.uuid == f'{file.uuid}.content':
            try:
                content = get_file_contents(api, item.hash)
            except:
                break
            if not isinstance(content, dict):
                break
        if item.uuid == f'{file.uuid}.metadata':
            if (old_document_collection := api.document_collections.get(file.uuid)) is not None:
                if api.document_collections[file.uuid].metadata.hash == item.hash:
                    if file.uuid in deleted_document_collections_list:
                        deleted_document_collections_list.remove(file.uuid)
                    if old_document_collection.uuid not in document_collections_with_items:
                        old_document_collection.has_items = False
                    if (parent_document_collection := api.document_collections.get(
                            old_document_collection.parent)) is not None:
                        parent_document_collection.has_items = True
                        document_collections_with_items.add(old_document_collection.parent)
                    continue
            elif (old_document := api.documents.get(file.uuid)) is not None:
                if api.documents[file.uuid].metadata.hash == item.hash:
                    if file.uuid in deleted_documents_list:
                        deleted_documents_list.remove(file.uuid)
                    if (parent_document_collection := api.document_collections.get(
                            old_document.parent)) is not None:
                        parent_document_collection.has_items = True
                    document_collections_with_items.add(old_document.parent)
                    continue
            try:
                metadata = models.Metadata(get_file_contents(api, item.hash), item.hash)
            except:
                continue
            if metadata.type == DocumentTypes.Collection.value:
                if content is not None:
                    tags = content.get('tags', ())
                else:
                    tags = ()
                api.document_collections[file.uuid] = models.DocumentCollection(
                    [models.Tag(tag) for tag in tags],
                    metadata, file.uuid
                )

                if file.uuid in document_collections_with_items:
                    api.document_collections[file.uuid].has_items = True

                if (parent_document_collection := api.document_collections.get(
                        api.document_collections[file.uuid].parent)) is not None:
                    parent_document_collection.has_items = True
                document_collections_with_items.add(api.document_collections[file.uuid].parent)

                if file.uuid in deleted_document_collections_list:
                    deleted_document_collections_list.remove(file.uuid)
                break
            elif metadata.type == 'DocumentType':
                api.documents[file.uuid] = models.Document(api,
                                                           models.Content(content, metadata, item.hash, api.debug),
                                                           metadata, file_content, file.uuid, file.hash)
                if not matches_hash:
                    badly_hashed.append(api.documents[file.uuid])
                if (parent_document_collection := api.document_collections.get(
                        api.documents[file.uuid].parent)) is not None:
                    parent_document_collection.has_items = True
                document_collections_with_items.add(api.documents[file.uuid].parent)
                if file.uuid in deleted_documents_list:
                    deleted_documents_list.remove(file.uuid)
                break


def get_documents_using_root(api: 'API', progress, root):
    progress(0, 1)
    try:
        if root == 'miss':
            print(
                f"{Fore.GREEN}{Style.BRIGHT}"
                f"Creating new root file."
                f"{Fore.RESET}{Style.RESET_ALL}"
            )
            api.reset_root()
            root = api.get_root().get('hash', 'miss')
            return get_documents_using_root(api, progress, root)
        _, files = get_file(api, root)
        if _ == -1 or len(files) == 0:  # Blank root file / Missing
            if api.offline_mode and _ == -1:
                api.spread_event(APIFatal())
                print(
                    f"{Fore.RED}{Style.BRIGHT}"
                    f"API is in offline mode, please sync at least once"
                    f"{Fore.RESET}{Style.RESET_ALL}"
                )
                return
            _, files = get_file(api, root, False)
    except DownloadOperation.DownloadCancelException:
        return
    except AssertionError as e:
        raise e  # Allow AssertionError passthrough
    except:  # Any network or read issue
        print_exc()
        from rm_api.storage.old_sync import update_root
        print(f"{Fore.RED}{Style.BRIGHT}AN ISSUE OCCURRED GETTING YOUR ROOT INDEX!{Fore.RESET}{Style.RESET_ALL}")

        api.reset_root()
    deleted_document_collections_list = set(api.document_collections.keys())
    deleted_documents_list = set(api.documents.keys())
    document_collections_with_items = set()
    badly_hashed = []

    total = len(files)
    count = 0
    progress(0, total)

    def handle_file(file: 'File'):
        nonlocal count, total
        _, file_content = get_file(api, file.hash)

        # Check the hash in case it needs fixing
        document_file_hash = sha256()
        for item in sorted(file_content, key=lambda item: file.uuid):
            document_file_hash.update(bytes.fromhex(item.hash))
        expected_hash = document_file_hash.hexdigest()
        matches_hash = file.hash == expected_hash

        process_file_content(file_content, file, deleted_document_collections_list, deleted_documents_list,
                             document_collections_with_items, badly_hashed, api, matches_hash)
        count += 1

        progress(count, total)

    def handle_file_and_check_for_errors(file: 'File'):
        try:
            handle_file(file)
        except:
            print_exc()

    try:
        # with ThreadPoolExecutor(max_workers=100) as executor:
        #     executor.map(handle_file_and_check_for_errors if api.debug else handle_file, files)
        #     executor.shutdown(wait=True)
        batch_size = 100
        with ThreadPoolExecutor() as executor:
            for batch in batched(files, batch_size):
                executor.map(handle_file_and_check_for_errors if api.debug else handle_file, batch)
    except RuntimeError:
        return
    i = 0

    if badly_hashed:
        print(f"{Fore.YELLOW}Warning, fixing some bad document tree hashes!{Fore.RESET}")
        api.upload_many_documents(badly_hashed)

    total += len(deleted_document_collections_list) + len(deleted_documents_list)

    for j, uuid in enumerate(deleted_document_collections_list):
        del api.document_collections[uuid]
        progress(i + j + 1, total)
    else:
        j = 0

    for k, uuid in enumerate(deleted_documents_list):
        try:
            if not api.documents[uuid].provision:
                del api.documents[uuid]
        except KeyError:
            pass
        progress(i + j + k + 1, total)
