from pathlib import Path

from ingest_router import _safe_upload_name, _write_uploaded_file


class DummyUpload:
    def __init__(self, name: str, payload: bytes = b"abc"):
        self.name = name
        self._payload = payload

    def getbuffer(self):
        return self._payload


def test_safe_upload_name_strips_path_traversal_and_special_chars():
    safe = _safe_upload_name("../../etc/passwd\\evil?.xlsx")
    assert "/" not in safe
    assert "\\" not in safe
    assert ".." not in safe
    assert safe.endswith(".xlsx")


def test_write_uploaded_file_stays_inside_tmp_dir(tmp_path):
    upload = DummyUpload("..\\..//outside.json", b"{}")
    out = _write_uploaded_file(upload, str(tmp_path))
    out_path = Path(out).resolve()
    assert out_path.parent == tmp_path.resolve()
    assert out_path.exists()
    assert out_path.read_bytes() == b"{}"
