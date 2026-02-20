import importlib
import sys

import pytest


def test_legacy_parser_shim_reexports_and_warns():
    legacy_name = "parse_" + "trak" + "sys"
    sys.modules.pop(legacy_name, None)

    with pytest.warns(DeprecationWarning):
        legacy = importlib.import_module(legacy_name)

    modern = importlib.import_module("parse_mes")

    assert hasattr(legacy, "detect_file_type")
    assert legacy.detect_file_type is modern.detect_file_type
