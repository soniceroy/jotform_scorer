import json
import pytest
from jotform_summary.csv_mapping import Loader

# @pytest.fixture
# def manifest():
#     with open("test_data/manifest.json") as f:
#         data = json.load(f)
#     return data


def test_cell_mapped_to_string():
    manifest = {"cargo": [
        {
            "load_type": "scalar",
            "col_num": 1,
            "row_num": 1,
            "map": {"map_type": "static_string", "output": "to you"}
        }
    ]}
    rows = [
        ["dang", "happy birthday"],
        ["on it", "to you"]
    ]
    loader = Loader(manifest, rows)
    assert(loader.get_string() == "to you")

def test_cell_mapped_to_string_with_label():
    manifest = {"cargo": [
        {
            "load_type": "scalar",
            "label": "happy birthday ",
            "col_num": 1,
            "row_num": 1,
            "map": {"map_type": "static_string", "output": "to you"}
        }
    ]}
    rows = [
        ["dang", "happy birthday"],
        ["on it", "to you"]
    ]
    loader = Loader(manifest, rows)
    assert(loader.get_string() == "happy birthday to you")


def test_cell_mapped_to_string_with_label_and_label_suffix():
    manifest = {"cargo": [
        {
            "load_type": "scalar",
            "label": "happy birthday",
            "label_suffix": " ",
            "col_num": 1,
            "row_num": 1,
            "map": {"map_type": "static_string", "output": "to you"}
        }
    ]}
    rows = [
        ["dang", "happy birthday"],
        ["on it", "to you"]
    ]
    loader = Loader(manifest, rows)
    assert(loader.get_string() == "happy birthday to you")


def test_string_from_col_when_map_omitted_and_no_label_with_suffix():
    manifest = {"cargo": [
        {
            "load_type": "scalar",
            "label_suffix": " to you",
            "col_num": 1,
            "row_num": 1,
        }
    ]}
    rows = [
        ["dang", "happy birthday"],
        ["on it", "nope"]
    ]
    loader = Loader(manifest, rows)
    assert(loader.get_string() == "happy birthday to you")

def test_string_from_col_when_map_omitted_label_is_ignored():
    manifest = {"cargo": [
        {
            "load_type": "scalar",
            "label": "hey, ",
            "label_suffix": " to you",
            "col_num": 1,
            "row_num": 1,
        }
    ]}
    rows = [
        ["dang", "happy birthday"],
        ["on it", "nope"]
    ]
    loader = Loader(manifest, rows)
    assert(loader.get_string() == "happy birthday to you")

def test_group_reduce_by_sum_to_string_with_label():
    manifest = {"cargo": [
        {
            "load_type": "group",
            "label": "group sums to: ",
            "row_num": 1,
            "start_col": 0,
            "end_col": 1,
            "reduce": "sum"
        }
    ]}

    rows = [
        ["column_name", ""],
        [ 21, 21 ]
    ]
    loader = Loader(manifest, rows)
    assert(loader.get_string() == "group sums to: 42.0")

def test_group_reduce_by_multiple_to_string():
    manifest = {"cargo": [
        {
            "load_type": "group",
            "label": "group multiplies to: ",
            "row_num": 1,
            "start_col": 0,
            "end_col": 1,
            "reduce": "multiple"
        }
    ]}

    rows = [
        ["column_name", ""],
        [ 6, 7 ]
    ]
    loader = Loader(manifest, rows)
    assert(loader.get_string() == "group multiplies to: 42.0")

def test_group_reduce_by_average_to_string():
    manifest = {"cargo": [
        {
            "load_type": "group",
            "label": "group averages to: ",
            "row_num": 1,
            "start_col": 0,
            "end_col": 1,
            "reduce": "average"
        }
    ]}

    rows = [
        ["column_name", ""],
        [ 21, 63 ]
    ]
    loader = Loader(manifest, rows)
    assert(loader.get_string() == "group averages to: 42.0")

def test_text_to_binary_scalar_map_to_one():
    manifest = {"preload": [
        {
            "col_num": 0,
            "row_num": 1,
            "map": {"map_type": "binary", "is_one": "true"}
        }],
        "cargo": []
    }
    rows = [
        ["dang", "happy birthday"],
        ["true", "nope"]
    ]
    loader = Loader(manifest, rows)
    assert(loader.rows[1][0] == 1)

def test_text_to_binary_scalar_map_to_zero():
    manifest = {"preload": [
        {
            "col_num": 0,
            "row_num": 1,
            "map": {"map_type": "binary", "is_one": "true"}
        }],
        "cargo": []
    }
    rows = [
        ["dang", "happy birthday"],
        ["not tru", "nope"]
    ]
    loader = Loader(manifest, rows)
    assert(loader.rows[1][0] == 0)