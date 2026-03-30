from PIL import Image

import ocr_utils


def test_normalize_ocr_text_collapses_whitespace(monkeypatch):
    monkeypatch.setattr("config.SEARCH_OCR_MIN_CHARS", 3)
    monkeypatch.setattr("config.SEARCH_OCR_MAX_CHARS", 2000)

    assert ocr_utils.normalize_ocr_text("  Hello \n\n  Rimrock\tOCR  ") == "Hello Rimrock OCR"


def test_prepare_ocr_image_downscales_large_images(monkeypatch):
    monkeypatch.setattr("config.SEARCH_OCR_MAX_DIM", 1000)

    img = Image.new("RGB", (4000, 2000), "white")
    prepared = ocr_utils.prepare_ocr_image(img)

    assert prepared.mode == "L"
    assert prepared.size == (1000, 500)


def test_load_local_image_rgb_allows_large_images_and_restores_guard(monkeypatch, tmp_path):
    monkeypatch.setattr("config.SEARCH_OCR_MAX_DIM", 640)

    original_guard = ocr_utils.Image.MAX_IMAGE_PIXELS

    class _FakeImage:
        size = (20000, 10000)

        def __init__(self):
            self.draft_args = None
            self.convert_mode = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def draft(self, mode, size):
            self.draft_args = (mode, size)

        def convert(self, mode):
            self.convert_mode = mode
            return {"mode": mode}

    fake = _FakeImage()
    monkeypatch.setattr(ocr_utils.Image, "open", lambda path: fake)

    loaded = ocr_utils.load_local_image_rgb(tmp_path / "oversized.jpg")

    assert loaded == {"mode": "RGB"}
    assert fake.draft_args == ("RGB", (640, 640))
    assert fake.convert_mode == "RGB"
    assert ocr_utils.Image.MAX_IMAGE_PIXELS == original_guard


def test_extract_ocr_text_uses_tesseract_output(monkeypatch):
    monkeypatch.setattr(ocr_utils, "resolve_tesseract_binary", lambda: "tesseract")

    class _Proc:
        returncode = 0
        stdout = "Invoice\n8472\n"
        stderr = ""

    monkeypatch.setattr(ocr_utils.subprocess, "run", lambda *args, **kwargs: _Proc())

    img = Image.new("RGB", (32, 32), "white")
    assert ocr_utils.extract_ocr_text(img) == "Invoice 8472"
