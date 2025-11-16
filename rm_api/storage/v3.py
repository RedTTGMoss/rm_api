import _thread
import asyncio
import base64
import json
import os
import pickle
import ssl
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from hashlib import sha256
from io import BytesIO
from json import JSONDecodeError
from traceback import format_exc, print_exc
from typing import TYPE_CHECKING, Union, Tuple, List, Set

import aiohttp
import certifi
import dill
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
    from rm_api.models import File, Document

DEFAULT_ENCODING = 'utf-8'
EXTENSION_ORDER = ['content', 'metadata', 'rm']


# if os.name == 'nt':
#     asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

class CacheMiss(Exception):
    pass


def pickle_document(doc: Union['models.Document', 'models.DocumentCollection']):
    if isinstance(doc, models.DocumentCollection):
        return dill.dumps(doc, fix_imports=True)
    api = doc.api
    doc.api = None
    data = pickle.dumps(doc, fix_imports=True)
    doc.api = api
    return data


def unpickle_document(data: bytes, api: 'API') -> Union['models.Document', 'models.DocumentCollection']:
    doc = pickle.loads(data, fix_imports=True)
    if isinstance(doc, models.Document):
        doc.api = api
    return doc


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
    if use_cache and api.indexer.hash_exists(file):
        operation.total = api.indexer.get_size(file)
        if head:
            operation.stage = FETCH_CACHE
            api.poll_download_operation(operation)
            return True
        operation.stage = LOAD_CONTENT
        api.begin_download_operation(operation)
        if binary:
            cache_data = api.indexer.read_bytes(file)
        else:
            data = api.indexer.read_string(file)
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

    if api.indexer.allow_write:
        try:
            with api.indexer.open_writer(file) as f:
                f.write(operation.first_chunk)
                for chunk in operation.iter():
                    f.write(chunk)
        except DownloadOperation.DownloadCancelException as e:
            if api.indexer.hash_exists(file):
                api.indexer.erase(file)
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
    """This function handles entirely parsing a document or collection from its file list."""

    # Because metadata and content files take time to parse, we check if we have a pickle cache first
    pickle_hash = f'{file.hash}.pickle'
    pickle_uuid_hash = f'{file.uuid}.pickle'
    require_update_cache = True
    if api.indexer.hash_exists(pickle_uuid_hash):
        # We have an old pickle cache, we need to remove it
        old_pickle_hash = api.indexer.read_string(pickle_uuid_hash)
        if api.indexer.hash_exists(old_pickle_hash) and old_pickle_hash != pickle_hash:
            api.indexer.erase(old_pickle_hash)
        else:
            require_update_cache = False

    def handle_document(doc):
        # We create and register the document
        # We also parse full document contents here into a Content object
        api.documents[file.uuid] = doc

        # Save to pickle cache for faster loading next time
        if require_update_cache:
            api.indexer.write_bytes(pickle_hash, pickle_document(api.documents[file.uuid]))
            api.indexer.write_string(pickle_uuid_hash, pickle_hash)

        # If the hash doesn't match, we add it to the list for fixing later
        if not matches_hash:
            badly_hashed.append(api.documents[file.uuid])

        # Mark the parent collection as having items since it does
        if (parent_document_collection := api.document_collections.get(
                api.documents[file.uuid].parent)) is not None:
            parent_document_collection.has_items = True
        document_collections_with_items.add(api.documents[file.uuid].parent)

        # Unmark it from the deleted list if it exists
        if file.uuid in deleted_documents_list:
            deleted_documents_list.remove(file.uuid)

    def handle_document_collection(doc_collection):
        # We create and register the document collection
        api.document_collections[file.uuid] = doc_collection

        # Save to pickle cache for faster loading next time
        if require_update_cache:
            api.indexer.write_bytes(pickle_hash, pickle.dumps(api.document_collections[file.uuid]))
            api.indexer.write_string(pickle_uuid_hash, pickle_hash)

        # Mark that this collection has items if applicable
        if file.uuid in document_collections_with_items:
            api.document_collections[file.uuid].has_items = True

        # Mark the parent collection as having items since it does
        if (parent_document_collection := api.document_collections.get(
                api.document_collections[file.uuid].parent)) is not None:
            parent_document_collection.has_items = True
        document_collections_with_items.add(api.document_collections[file.uuid].parent)

        # Unmark it from the deleted list if it exists
        if file.uuid in deleted_document_collections_list:
            deleted_document_collections_list.remove(file.uuid)


    # Check for pickle cache first
    if api.indexer.hash_exists(pickle_hash):
        data = api.indexer.read_bytes(pickle_hash)
        obj = unpickle_document(data, api)
        if isinstance(obj, models.DocumentCollection):
            handle_document_collection(obj)
        elif isinstance(obj, models.Document):
            handle_document(obj)
        return  # We loaded from cache, no need to continue processing

    content = None

    for item in file_content:
        # If we match the content file, we just store it for later
        if item.uuid == f'{file.uuid}.content':
            try:
                content = get_file_contents(api, item.hash)
            except:
                break
            if not isinstance(content, dict):
                break
        if item.uuid == f'{file.uuid}.metadata':
            #
            # Document/Collection here is already confirmed, run checks on existing items
            #
            # First we check if the file matches an existing document collection
            if (old_document_collection := api.document_collections.get(file.uuid)) is not None:
                if api.document_collections[file.uuid].metadata.hash == item.hash:
                    # We check and remove the item from the deleted list if it exists
                    if file.uuid in deleted_document_collections_list:
                        deleted_document_collections_list.remove(file.uuid)

                    # We also reset has_items if it doesn't have items
                    if old_document_collection.uuid not in document_collections_with_items:
                        old_document_collection.has_items = False

                    # Finally, we mark the parent collection as having items since it does
                    if (parent_document_collection := api.document_collections.get(
                            old_document_collection.parent)) is not None:
                        parent_document_collection.has_items = True
                        document_collections_with_items.add(old_document_collection.parent)

                    # This document collection is already present and up to date, skip processing
                    continue
            # Next we check if the file matches an existing document
            elif (old_document := api.documents.get(file.uuid)) is not None:
                if api.documents[file.uuid].metadata.hash == item.hash:
                    # We check and remove the item from the deleted list if it exists
                    if file.uuid in deleted_documents_list:
                        deleted_documents_list.remove(file.uuid)
                    # Finally, we mark the parent collection as having items since it does
                    if (parent_document_collection := api.document_collections.get(
                            old_document.parent)) is not None:
                        parent_document_collection.has_items = True
                    document_collections_with_items.add(old_document.parent)
                    continue

            #
            # Begin processing the metadata file
            #

            try:
                # We finally process the raw metadata into a Metadata object
                metadata = models.Metadata(get_file_contents(api, item.hash), item.hash)
            except:
                # If there's an error processing the metadata, we can just skip it, it's no use
                continue
            if metadata.type == DocumentTypes.Collection.value:
                # Extract tags if present, collections only contain tags in their content file
                if content is not None:
                    tags = content.get('tags', ())
                else:
                    tags = ()

                # We create and register the document collection
                doc = models.DocumentCollection(
                    [models.Tag(tag) for tag in tags],
                    metadata, file.uuid
                )

                handle_document_collection(doc)
                break  # Finish processing this file, there is no need to continue
            elif metadata.type == DocumentTypes.Document.value:
                # We create and register the document
                # We also parse full document contents here into a Content object
                doc = models.Document(api,
                                      models.Content(content, metadata, item.hash, api.debug),
                                      metadata, file_content, file.uuid, file.hash)
                handle_document(doc)
                break  # Finish processing this file, there is no need to continue

        # Any other files here can be skipped, they aren't relevant


def get_documents_using_root(api: 'API', progress, root):
    progress(0, 1)
    # This initial part is entirely delegated to handling the root file and any issues
    try:
        if root == 'miss':  # Missing root file
            print(
                f"{Fore.GREEN}{Style.BRIGHT}"
                f"Creating new root file."
                f"{Fore.RESET}{Style.RESET_ALL}"
            )
            api.reset_root()
            root = api.get_root().get('hash', 'miss')
            return get_documents_using_root(api, progress, root)
        version, files = get_file(api, root)  # Fetch the root file
        if version == -1 or len(files) == 0:  # Blank root file / Missing
            if api.offline_mode and version == -1:  # Offline and can't get root
                api.spread_event(APIFatal())
                progress(0, 0)
                print(
                    f"{Fore.RED}{Style.BRIGHT}"
                    f"API is in offline mode, please sync at least once"
                    f"{Fore.RESET}{Style.RESET_ALL}"
                )
                return
            version, files = get_file(api, root, False)  # Force refetch
    except DownloadOperation.DownloadCancelException:  # Cancelled by user
        progress(0, 0)
        return
    except AssertionError as e:
        progress(0, 0)
        raise e  # Allow AssertionError passthrough
    except:  # Any network or read issue
        print_exc()
        api.spread_event(APIFatal())
        progress(0, 0)
        print(f"{Fore.RED}{Style.BRIGHT}AN ISSUE OCCURRED GETTING YOUR ROOT INDEX!{Fore.RESET}{Style.RESET_ALL}")
        return

    # We make a list of all the documents and collections we have
    # These dictionaries store any file that might be deleted
    # If these items remain in the lists at the end of processing, they are deleted from the api
    # If the files still exist, then they are removed from these lists to avoid deletion
    deleted_document_collections_list = set(api.document_collections.keys())
    deleted_documents_list = set(api.documents.keys())

    # We also keep track of collections with items, so we can mark them correctly
    document_collections_with_items = set()
    badly_hashed = []

    # We mark the progress of the fetch
    total = len(files)
    count = 0
    progress(0, total)

    def handle_file(file: 'File'):  # This refers to one file from the root
        nonlocal count, total
        _, file_content = get_file(api, file.hash)  # Get the file content listing

        # Check the hash in case it needs fixing
        document_file_hash = sha256()
        for item in sorted(file_content, key=lambda item: file.uuid):
            document_file_hash.update(bytes.fromhex(item.hash))
        expected_hash = document_file_hash.hexdigest()
        matches_hash = file.hash == expected_hash

        # Process the document/collection
        process_file_content(file_content, file, deleted_document_collections_list, deleted_documents_list,
                             document_collections_with_items, badly_hashed, api, matches_hash)
        count += 1

        progress(count, total)  # The file is finished and we report progress

    def handle_file_and_check_for_errors(file: 'File'):
        # This is a wrapper function to catch potentially corrupted files or code errors,
        # without halting the entire sync process
        try:
            handle_file(file)
        except:
            print_exc()

    # Run a threading pool to handle files in parallel
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

    # Here we use the api to reupload any badly hashed documents
    # This process goes through rm_api's normal upload process to ensure everything is correct
    # This includes recalculating hashes and reuploading files and updating the root
    # It's important to note that this isn't important and resyncing isn't necessary here
    if badly_hashed:
        print(f"{Fore.YELLOW}Warning, fixing some bad document tree hashes!{Fore.RESET}")
        api.upload_many_documents(badly_hashed)

    # We add additional set of steps for deleting any documents or collections that are no longer present
    total += len(deleted_document_collections_list) + len(deleted_documents_list)

    # Finally we go through and delete any remaining items in the deleted lists
    for _, uuid in enumerate(deleted_document_collections_list):
        try:
            del api.document_collections[uuid]
        except KeyError:
            pass
        count += 1
        progress(count, total)

    for _, uuid in enumerate(deleted_documents_list):
        try:
            if not api.documents[uuid].provision:
                del api.documents[uuid]
        except KeyError:
            pass
        count += 1
        progress(count, total)

    # We fetched / fixed / reloaded the root file
    # We parsed through each file, creating documents and collections as necessary
    # We deleted any documents or collections that were no longer present
    # Finally if everything went well, the progress has already reached 100%
