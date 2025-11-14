import os.path
import shutil
import time
from os import mkdir
from threading import Thread

import rm_api
from slashr import SlashR
from rm_api import API
from rm_api.storage.v3 import poll_file, check_file_exists

print(rm_api.__file__)

api = API()

_ = api.token

def get_documents(d):
    def progress(done, total):
        d['done'] = done
        d['total'] = total
    start = time.time()
    api.get_documents(progress)
    print(f"Fetching documents took {time.time()-start} seconds")

def measure(msg):
    with SlashR(False) as sr:
        print(msg)
        d = {'done': 0, 'total': 0}
        Thread(target=get_documents, args=(d,)).start()
        while d['total'] <= 0:
            time.sleep(0.1)
        while d['done'] < d['total']:
            sr.print(f"Downloaded {d['done']} / {d['total']}")
            time.sleep(0.1)
        sr.print(f"Downloaded {d['done']} / {d['total']}")

if len(os.listdir(api.sync_file_path)) > 0:
    shutil.rmtree(api.sync_file_path)
    mkdir(api.sync_file_path)

measure("Now cold downloading")
measure("Now live downloading")
api = API()
poll_file.cache_clear()
check_file_exists.cache_clear()
measure("Now hot downloading")