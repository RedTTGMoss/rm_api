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

api = API(ask_reset=True)
api.debug = True

_ = api.token

api.get_documents()
api.indexer.log_and_reset_stats()