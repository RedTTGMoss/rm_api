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

if TYPE_CHECKING:
    from . import API


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
        operation = DownloadOperation(ref)
        api.begin_download_operation(operation)
        kwargs['operation'] = operation
        try:
            data = fn(api, *args, **kwargs)
        except DownloadOperation.DownloadCancelException:
            api.log(f'DOWNLOAD CANCELLED\n{Fore.LIGHTBLACK_EX}{format_exc()}{Fore.RESET}')
            raise
        operation.finish()
        api.finish_download_operation(operation)
        return data

    return wrapped
