import json
import threading
from traceback import print_exc
from typing import TYPE_CHECKING

import websocket
from colorama import Fore

from rm_api.notifications.models import *

if TYPE_CHECKING:
    from rm_api import API

NOTIFICATION_URL = "{0}notifications/ws/json/1"


def _on_message(api: 'API', message: str):
    message = json.loads(message)
    message_event = message['message']['attributes']['event']
    if message_event == 'SyncComplete':
        api.spread_event(SyncCompleted(message['message']))
    else:
        print(f"{Fore.YELLOW}Warning unknown notification event: {message_event}{Fore.RESET}")


def on_message(api: 'API', message: str):
    try:
        _on_message(api, message)
    except Exception as e:
        print_exc()


def _listen(api: 'API'):
    ws_url = NOTIFICATION_URL.format(api.document_notifications_uri)
    if ws_url.startswith("https://"):
        ws_url = ws_url.replace("https://", "wss://", 1)
    elif ws_url.startswith("http://"):
        ws_url = ws_url.replace("http://", "ws://", 1)
    ws = websocket.WebSocketApp(
        ws_url, on_message=lambda _, msg: on_message(api, msg),
        header={"Authorization": f"Bearer {api.token}"}
    )
    ws.run_forever(
        ping_interval=10,
        reconnect=5
    )


def handle_notifications(api: 'API'):
    threading.Thread(target=_listen, args=(api,), daemon=True).start()
