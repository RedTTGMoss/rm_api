import time

from slashr import SlashR
from rm_api import API, make_files_request, DownloadOperation, check_file_exists, get_file_contents
from rm_api.notifications.models import Notification, LongLasting
from rm_api.storage.exceptions import NewSyncRequired
from rm_api.storage.v3 import poll_file, get_file

S = 'https://rmcloud.redttg.com'

api = API(uri=S, discovery_uri=S)
api.debug = True
with SlashR(False) as sr:
    api.get_documents(lambda d, t: sr.print(f"Downloading documents... {d}/{t}"))
root = api.get_root()['hash']

def normal(size: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1000:
            return f"{size:.2f} {unit}"
        size /= 1000
    return f"{size:.2f} PB"


def debug_hook(event: Notification | LongLasting):
    if isinstance(event, DownloadOperation.DownloadOperationEvent) and event.operation.total > 1000 * 1000:
        print(f"[{event.__class__.__name__}] Download operation: {event.operation.ref} - "
              f"Stage: {event.operation.stage} - "
              f"Done: {normal(event.operation.done)} / {normal(event.operation.total)}")


# api.add_hook('debug', debug_hook)
for document in api.documents.values():
    document.ensure_download_and_callback(lambda: None)

    while not document.download_total:
        pass

    try:
        with SlashR(False, padding_amount=1, padding_string='==| ') as sr:
            while document.downloading:
                sr.print(f"Downloading {document.metadata.visible_name} {normal(document.download_done)}/{normal(document.download_total)}")
                time.sleep(0.1)
    except KeyboardInterrupt:
        api.force_stop_all()
        sr.print(f"Downloading CANCELED")
        raise StopIteration()