from functools import wraps
from io import BytesIO
from itertools import islice
from threading import Thread
from traceback import format_exc
from typing import TYPE_CHECKING, Set

from PyPDF2 import PdfReader
from colorama import Fore

from rm_api.notifications.models import DownloadOperation
from rm_api.storage.common import FileHandle
from rm_api.sync_stages import UNKNOWN_DOWNLOAD_OPERATION, FETCH_FILE

if TYPE_CHECKING:
    from . import API, File


def get_pdf_page_count(pdf: bytes):
    if isinstance(pdf, FileHandle):
        reader = PdfReader(pdf)
    else:
        reader = PdfReader(BytesIO(pdf))

    return len(reader.pages)


def threaded(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        thread = Thread(target=fn, args=args, kwargs=kwargs, daemon=True)
        thread.start()
        return thread

    return wrapper


def batched(iterable, batch_size):
    it = iter(iterable)
    while batch := list(islice(it, batch_size)):
        yield batch


def download_operation_wrapper(fn):
    @wraps(fn)
    def wrapped(api: 'API', *args, **kwargs):
        ref = kwargs.get('ref')  # Download operation reference, for example document or collection
        stage = kwargs.get('stage')
        update = kwargs.get('update')
        auto_finish = kwargs.get('auto_finish', True)
        if ref is not None:
            del kwargs['ref']
        if kwargs.get('stage') is not None:
            del kwargs['stage']
        if kwargs.get('auto_finish') is not None:
            del kwargs['auto_finish']
        if update is not None:
            del kwargs['update']
        operation = kwargs.get('operation') or DownloadOperation(ref)
        operation.stage = stage or operation.stage
        operation.update_ref = update or operation.update_ref
        api.add_download_operation(operation)
        if update:
            getattr(update, 'add_download_operation')(operation)
        kwargs['operation'] = operation
        try:
            data = fn(api, *args, **kwargs)
        except DownloadOperation.DownloadCancelException:
            if update:
                getattr(update, 'remove_download_operation')(operation)
            api.log(f'DOWNLOAD CANCELLED\n{Fore.LIGHTBLACK_EX}{format_exc()}{Fore.RESET}')
            raise
        except:
            api.cancel_download_operation(operation)
            if update:
                getattr(update, 'remove_download_operation')(operation)
            raise
        if auto_finish and (operation.done >= operation.total or operation.stage == FETCH_FILE):
            if update:
                getattr(update, 'remove_download_operation')(operation)
            api.finish_download_operation(operation)
        return data

    return wrapped


def download_operation_wrapper_with_stage(stage: int):
    """
    Decorator to wrap a function with a download operation and a specific stage.
    """

    def decorator(fn):
        wrapped_fn = download_operation_wrapper(fn)

        @wraps(wrapped_fn)
        def wrapped(*args, **kwargs):
            if 'stage' not in kwargs:
                kwargs['stage'] = stage
            return wrapped_fn(*args, **kwargs)

        return wrapped

    return decorator


class DownloadOperationsSupport:
    def __init__(self):
        self._download_operations: Set[DownloadOperation] = set()

    def add_download_operation(self, operation: DownloadOperation):
        self._download_operations.add(operation)

    def remove_download_operation(self, operation: DownloadOperation):
        self._download_operations.remove(operation)

    @property
    def downloading(self):
        if len(self._download_operations) == 0:
            return False
        return any(not op.finished for op in list(self._download_operations) if not op.canceled)

    @property
    def download_done(self):
        return sum(op.done for op in list(self._download_operations) if not op.canceled)

    @property
    def download_total(self):
        return sum(op.total for op in list(self._download_operations) if not op.canceled)
