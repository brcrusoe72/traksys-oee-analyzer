from pathlib import Path
import shutil
import uuid

import pytest


_BASE = Path(__file__).resolve().parent / ".test_tmp_local"


@pytest.fixture(scope="session")
def tmp_path_factory():
    class _Factory:
        def mktemp(self, basename: str, numbered: bool = True):
            _BASE.mkdir(parents=True, exist_ok=True)
            suffix = uuid.uuid4().hex[:8] if numbered else ""
            name = f"{basename}_{suffix}" if suffix else basename
            path = _BASE / name
            path.mkdir(parents=True, exist_ok=False)
            return path

    return _Factory()


@pytest.fixture
def tmp_path(tmp_path_factory):
    path = tmp_path_factory.mktemp("pytest")
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)
