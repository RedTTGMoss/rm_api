class NewSyncRequired(Exception):
    def __init__(self):
        super().__init__("The client attempted to use the old sync API, but the server requires the new sync API.")


class ThreadBreak(Exception):
    def __init__(self):
        super().__init__("Thread break.")
