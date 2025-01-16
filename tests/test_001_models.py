from rm_api import models, make_hash
from .common import TestWithData
from ..defaults import ZoomModes


class TestModels(TestWithData):

    def test_001_handle_metadata(self):
        for raw_metadata in self.metadata_files.values():
            metadata = models.Metadata(raw_metadata, make_hash(raw_metadata))
            output = metadata.to_dict()
            self.assertEqual(raw_metadata, output, "Metadata should be the same")

            metadata.modify_now()
            raw_modified_metadata = {
                **raw_metadata,
                'last_modified': metadata.last_modified
            }

            output = metadata.to_dict()
            self.assertEqual(raw_metadata, output, "Metadata should be the same")

    def test_002_content_last_page_visited(self):
        content = self.make_content('last_page_visited')
        self.assertEqual(-1, content.cover_page_number, "Last page should be indexed as -1")

    def test_003_content_first_page(self):
        content = self.make_content('first_page')
        self.assertEqual(0, content.cover_page_number, "Last page should be indexed as -1")

    def test_004_zoom_mode(self):
        for mode, content_file_name in (
                (ZoomModes.FitToWidth, 'pdf_zoom_width'),
                (ZoomModes.FitToHeight, 'pdf_zoom_height'),
                (ZoomModes.CustomFit, 'pdf_zoom_custom'),
        ):
            content = self.make_content(content_file_name)
            output = content.to_dict()
            self.assertEqual(mode, content.zoom.zoom_mode,
                             f"Zoom mode should be {mode}")
            self.assertEqual(content._content, output, "Content should be the same")
