"""
This module provides a locking mechanism to manage concurrent download operations
The idea is to allow multiple small download operations to proceed concurrently
while preventing the total size of concurrent downloads from exceeding a specified limit.

Meaning large downloads will block until the rest are done.

THE LOCK SHOULD ONLY BE LOCKED WHEN BEGINNING A DOWNLOAD OPERATION
"""

import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rm_api import API
    from rm_api.notifications.models import DownloadOperation

MAX_LOCK_TOTAL = 100000000  # 100MB
LOCK_MIN_PASSTHROUGH = 1000000  # 1MB, minimum size to pass through the lock without waiting
MAX_TASKS = 20  # Allowing more tasks may lead to performance issues, so we limit it

class DownloadLockRequest:
    def __init__(self, lock: 'DownloadLock', download_operation: 'DownloadOperation'):
        self.lock = lock
        self.operation = download_operation
        self.total = download_operation.total

    def __enter__(self):
        with self.lock.condition:
            while self.lock.total >= MAX_LOCK_TOTAL and self.total < LOCK_MIN_PASSTHROUGH and self.lock.tasks > MAX_TASKS:
                self.lock.condition.wait()
                if self.lock.stopped or self.operation.canceled:
                    self.lock.tasks -= 1
                    if self.total >= LOCK_MIN_PASSTHROUGH:
                        self.lock.total -= self.total
                    raise self.operation.DownloadCancelException
            if self.total >= LOCK_MIN_PASSTHROUGH:
                self.lock.total += self.total
        with self.lock.task_condition:
            self.lock.tasks += 1
            self.lock.task_condition.notify_all()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self.lock.condition:
            if self.total >= LOCK_MIN_PASSTHROUGH:
                self.lock.total -= self.total
            self.lock.condition.notify_all()
        with self.lock.task_condition:
            self.lock.tasks -= 1
            self.lock.task_condition.notify_all()
        return False


class DownloadLock:
    def __init__(self, api: 'API'):
        self.api = api
        self.total = 0
        self.tasks = 0
        self.condition = threading.Condition()
        self.task_condition = threading.Condition()
        self.stopped = False

    def stop(self):
        with self.condition:
            self.stopped = True
            self.condition.notify_all()

    def __call__(self, download_operation: 'DownloadOperation') -> DownloadLockRequest:
        return DownloadLockRequest(self, download_operation)
