from functools import wraps
from io import BytesIO
from threading import Thread

from PyPDF2 import PdfReader

from rm_api.storage.common import FileHandle


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
