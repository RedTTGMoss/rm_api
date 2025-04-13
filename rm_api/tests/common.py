import json
import os
from unittest import TestCase

from rm_api import models, make_hash

script_folder = os.path.dirname(__file__)
files_folder = os.path.join(script_folder, 'files')
content_folder = os.path.join(files_folder, 'content')
metadata_folder = os.path.join(files_folder, 'metadata')


class TestWithData(TestCase):
    metadata_files = {}
    content_files = {}

    def setUp(self):
        if not self.metadata_files:
            self.load_metadata_files()
        if not self.content_files:
            self.load_content_files()

    def load_metadata_files(self):
        for filename in os.listdir(metadata_folder):
            with open(os.path.join(metadata_folder, filename), 'r') as f:
                self.metadata_files[filename.rsplit('.', 1)[0]] = json.load(f)

    def load_content_files(self):
        for filename in os.listdir(content_folder):
            with open(os.path.join(content_folder, filename), 'r') as f:
                self.content_files[filename.rsplit('.', 1)[0]] = json.load(f)

    def make_content(self, name):
        metadata = models.Metadata.new('random', None)
        return models.Content(self.content_files[name], metadata, make_hash(self.content_files[name]), True)
