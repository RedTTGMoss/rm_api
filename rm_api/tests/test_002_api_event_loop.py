import asyncio
import os
import tempfile
import unittest
from unittest.mock import patch

from rm_api import API


class TestAPIEventLoopInit(unittest.TestCase):

    def _make_api_without_token_io(self):
        token_path = os.path.join(tempfile.gettempdir(), "rm_api_missing_token_for_test")
        if os.path.exists(token_path):
            os.remove(token_path)
        with patch.object(API, "get_token", return_value=None):
            return API(require_token=False, token_file_path=token_path)

    def test_sync_context_creates_or_gets_loop(self):
        api = self._make_api_without_token_io()
        self.assertIsNotNone(api.loop)
        self.assertFalse(api.loop.is_closed())

    def test_running_loop_context_uses_active_loop(self):
        async def _build_api_inside_running_loop():
            running_loop = asyncio.get_running_loop()
            api = self._make_api_without_token_io()
            self.assertIs(api.loop, running_loop)

        asyncio.run(_build_api_inside_running_loop())

