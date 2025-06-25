"""
This module contains the models for the notifications.
Since these notifications are pretty mediocre, don't let the length of this file fool you.
"""
from io import BytesIO, TextIOWrapper
from typing import Union, TYPE_CHECKING, Iterator, Any

from requests import Response

from ..sync_stages import STAGE_START, UNKNOWN_DOWNLOAD_OPERATION

if TYPE_CHECKING:
    from ..models import DocumentCollection, Document


class Notification:  # A class to be used as a base class for all notifications
    ...


class LongLasting:  # A class to be used as a base class for all ongoing operation events
    ...


class SyncCompleted(Notification):
    """
    This event is used by the websocket to spread a sync completion,
    when received from remarkable cloud.
    """

    def __init__(self, message: dict):
        self.source_device_id = message['attributes'].get('sourceDeviceID')


class NewDocuments(Notification):
    """This event is issued when potential API.documents / API.document_collections changes occurred"""
    ...


class APIFatal(Notification):
    """
    This signals the code should stop executing commands to the api instantly to prevent damage.
    It is recommended to follow this event as if something went wrong, continuing might make it become worse!
    """
    ...


class SyncRefresh(SyncCompleted):
    """
    Used when new files were synced by moss / moss doesn't pick up sync complete.
    This will force a sync refresh to get the latest document information.
    """

    # noinspection PyMissingConstructor
    def __init__(self):
        self.source_device_id = None


class SyncProgressBase(LongLasting):
    finished: bool

    def __init__(self):
        self.done: int = 0
        self.total: int = 0
        self.stage: int = STAGE_START


class FileSyncProgress(SyncProgressBase):
    """This is a sync progress event meant for any sync operation"""

    def __init__(self):
        super().__init__()
        self.finished = False


class DocumentSyncProgress(SyncProgressBase):
    """This is a sync progress event meant for keeping track of a individual document sync"""

    def __init__(self, document_uuid: str, file_sync_operation: FileSyncProgress = None):
        self.document_uuid = document_uuid
        self.file_sync_operation = file_sync_operation
        self.total_tasks = 0
        self.finished_tasks = 0
        self._tasks_was_set_once = False
        super().__init__()

    @property
    def finished(self):
        if not self._tasks_was_set_once:
            return False
        return self.total_tasks - self.finished_tasks <= 0

    def add_task(self, count: int = 1):
        self._tasks_was_set_once = True
        self.total_tasks += count

    def finish_task(self):
        self.finished_tasks += 1
        if self.file_sync_operation:
            self.file_sync_operation.done += 1

class DocumentDownloadProgress(SyncProgressBase):
    """
    A sync progress event that automatically relays the download progress of a document.
    """

    # noinspection PyMissingConstructor
    def __init__(self, document: 'Document'):
        self.document = document

    @property
    def document_uuid(self):
        return self.document.uuid

    @property
    def done(self):
        return self.document.download_done

    @property
    def total(self):
        return self.document.download_total

    @property
    def finished(self):
        return not self.document.downloading

class DownloadOperation(SyncProgressBase):
    raw_read: BytesIO
    text_read: TextIOWrapper
    raw_read_iter: Iterator
    first_chunk: bytes

    def __init__(self, ref: Union['Document', 'DocumentCollection'], stage: int = UNKNOWN_DOWNLOAD_OPERATION,
                 update_ref: Any = None):
        super().__init__()
        self.canceled = False
        self.cancel_reason = None
        self.ref = ref
        self.update_ref = update_ref
        self.stage = stage
        self.finished = False

    def finish(self):
        self.finished = True

    def cancel(self, reason: str = "No reason provided"):
        self.canceled = True
        self.cancel_reason = reason

    def use_response(self, response: Response, head: bool = False):
        # Only grab the total size if it's a head request.
        self.total = int(response.headers.get('content-length'))
        self.done = 0
        if head:
            return
        self.raw_read = BytesIO()
        self.text_read = TextIOWrapper(self.raw_read, encoding="utf-8")
        self.raw_read_iter = response.iter_content(chunk_size=1024)

        self.first_chunk = self.next_chunk()

    def iter(self):
        for chunk in self.raw_read_iter:
            self.raw_read.write(chunk)
            self.done += len(chunk)
            if self.canceled:
                raise self.DownloadCancelException()
            yield chunk

    def next_chunk(self):
        chunk = next(self.raw_read_iter)
        self.raw_read.write(chunk)
        self.done += len(chunk)
        if self.canceled:
            raise self.DownloadCancelException()
        return chunk

    def get_text(self) -> str:
        self.text_read.flush()
        self.text_read.seek(0)
        return self.text_read.read()

    def get_bytes(self) -> bytes:
        self.raw_read.flush()
        return self.raw_read.getvalue()

    class DownloadCancelException(Exception):
        ...

    class DownloadOperationEvent(Notification):
        def __init__(self, operation):
            self.operation = operation

        @property
        def __dict__(self):
            return self.operation.__dict__

    class DownloadCancelEvent(DownloadOperationEvent):
        ...

    class DownloadPollEvent(DownloadOperationEvent):
        ...

    class DownloadBeginEvent(DownloadOperationEvent):
        ...

    class DownloadFinishEvent(DownloadOperationEvent):
        ...

    @property
    def cancel_event(self):
        return self.DownloadCancelEvent(self)

    @property
    def begin_event(self):
        return self.DownloadBeginEvent(self)

    @property
    def poll_event(self):
        return self.DownloadPollEvent(self)

    @property
    def finish_event(self):
        return self.DownloadFinishEvent(self)

    @property
    def __dict__(self):
        return {
            'ref': self.ref,
            'stage': self.stage,
            'done': self.done,
            'total': self.total,
            'finished': self.finished,
            'canceled': self.canceled,
            'cancel_reason': self.cancel_reason
        }
