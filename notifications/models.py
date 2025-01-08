"""
This module contains the models for the notifications.
Since these notifications are pretty mediocre, don't let the length of this file fool you.
"""


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
        self.done = 0
        self.total = 0


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
