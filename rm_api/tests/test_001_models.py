import os

from rm_api import models, make_hash
from .common import TestWithData, test_pdf_file, test_epub_file, test_files_folder
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
                "last_modified": metadata.last_modified,
            }

            output = metadata.to_dict()
            self.assertEqual(raw_metadata, output, "Metadata should be the same")

    def test_002_content_last_page_visited(self):
        content = self.make_content("last_page_visited")
        self.assertEqual(
            -1, content.cover_page_number, "Last page should be indexed as -1"
        )

    def test_003_content_first_page(self):
        content = self.make_content("first_page")
        self.assertEqual(
            0, content.cover_page_number, "Last page should be indexed as -1"
        )

    def test_004_zoom_mode(self):
        for mode, content_file_name in (
            (ZoomModes.FitToWidth, "pdf_zoom_width"),
            (ZoomModes.FitToHeight, "pdf_zoom_height"),
            (ZoomModes.CustomFit, "pdf_zoom_custom"),
        ):
            content = self.make_content(content_file_name)
            output = content.to_dict()
            self.assertEqual(
                mode, content.zoom.zoom_mode, f"Zoom mode should be {mode}"
            )
            self.assertEqual(content._content, output, "Content should be the same")

    # def test_005_local_document_new_notebook(self):
    #     temp_dir = self.create_temp_dir()
    #     document = models.LocalDocument.new_notebook("Test Notebook", temp_dir)
    #     try:
    #         document.export_and_save()
    #         self.fail(
    #             "Expected ValueError when exporting without specifying a directory and no local dir set"
    #         )
    #     except ValueError:
    #         pass
    #     document.export_and_save(temp_dir)
    #
    #     # Verify files
    #     expected_files = [
    #         os.path.join(
    #             f"{document.uuid}", f"{document.content.c_pages.pages[0].id}.rm"
    #         ),
    #         f"{document.uuid}.metadata",
    #         f"{document.uuid}.content",
    #     ]
    #
    #     for file in expected_files:
    #         file_path = os.path.join(temp_dir, file)
    #         self.assertTrue(
    #             os.path.exists(file_path), f"Expected file {file_path} to exist"
    #         )

    def test_006_local_document_new_pdf(self):
        temp_dir = self.create_temp_dir()
        document = models.LocalDocument.new_pdf("Test PDF", test_pdf_file, temp_dir)
        try:
            document.export_and_save()
            self.fail(
                "Expected ValueError when exporting without specifying a directory and no local dir set"
            )
        except ValueError:
            pass
        document.export_and_save(temp_dir)

        # Verify files
        expected_files = [
            f"{document.uuid}.pdf",
            f"{document.uuid}.metadata",
            f"{document.uuid}.content",
        ]

        for file in expected_files:
            file_path = os.path.join(temp_dir, file)
            self.assertTrue(
                os.path.exists(file_path), f"Expected file {file_path} to exist"
            )

    def test_007_local_document_new_epub(self):
        temp_dir = self.create_temp_dir()
        document = models.LocalDocument.new_epub("Test EPUB", test_epub_file, temp_dir)
        try:
            document.export_and_save()
            self.fail(
                "Expected ValueError when exporting without specifying a directory and no local dir set"
            )
        except ValueError:
            pass
        document.export_and_save(temp_dir)

        # Verify files
        expected_files = [
            f"{document.uuid}.epub",
            f"{document.uuid}.metadata",
            f"{document.uuid}.content",
        ]

        for file in expected_files:
            file_path = os.path.join(temp_dir, file)
            self.assertTrue(
                os.path.exists(file_path), f"Expected file {file_path} to exist"
            )

    def test_008_local_document_load_files(self):
        for file in os.listdir(test_files_folder):
            file_path = os.path.join(test_files_folder, file)
            doc = models.LocalDocument.load_rmdoc(file_path)
            self.assertIsInstance(
                doc,
                models.LocalDocument,
                "Loaded document should be an instance of LocalDocument",
            )
            for file in doc.files:
                file_path = doc.get_file(file.hash)
                self.assertTrue(
                    os.path.exists(file_path), f"Expected file {file_path} to exist"
                )
