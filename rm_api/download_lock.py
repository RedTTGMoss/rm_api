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
    from rm_api.notifications.models import DownloadOperation

MAX_LOCK_TOTAL = 10000000  # 10MB

class DownloadLockRequest:
    def __init__(self, lock: 'DownloadLock', download_operation: 'DownloadOperation'):
        self.lock = lock
        self.operation = download_operation
        self.total = download_operation.total

    def __enter__(self):
        with self.lock.condition:
            while self.lock.total >= MAX_LOCK_TOTAL:
                self.lock.condition.wait()
            self.lock.total += self.total
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self.lock.condition:
            self.lock.total -= self.total
            self.lock.condition.notify_all()
        return False


class DownloadLock:
    def __init__(self):
        self.total = 0
        self.condition = threading.Condition()

    def __call__(self, download_operation: 'DownloadOperation') -> DownloadLockRequest:
        return DownloadLockRequest(self, download_operation)