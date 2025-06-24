from functools import wraps
from io import BytesIO
from itertools import islice
from threading import Thread
from traceback import format_exc
from typing import TYPE_CHECKING

from PyPDF2 import PdfReader
from colorama import Fore

from rm_api.notifications.models import DownloadOperation
from rm_api.storage.common import FileHandle
from rm_api.sync_stages import UNKNOWN_DOWNLOAD_OPERATION

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
        stage = kwargs.get('stage', UNKNOWN_DOWNLOAD_OPERATION)
        update = kwargs.get('update')
        if ref is not None:
            del kwargs['ref']
        if kwargs.get('stage') is not None:
            del kwargs['stage']
        if update is not None:
            del kwargs['update']
        operation = DownloadOperation(ref, stage, update)
        if update:
            setattr(update, 'download_operation', operation)
        api.begin_download_operation(operation)
        kwargs['operation'] = operation
        try:
            data = fn(api, *args, **kwargs)
        except DownloadOperation.DownloadCancelException:
            api.log(f'DOWNLOAD CANCELLED\n{Fore.LIGHTBLACK_EX}{format_exc()}{Fore.RESET}')
            raise
        operation.finish()
        if update:
            setattr(update, 'download_operation', None)
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
