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

# def test_group_reduce_by_sum_to_string_with_label():
#     manifest = {"cargo": [
#         {
#             "load_type": "group",
#             "label": "column_sums to: ",
#             "row_num": 0,
#             "start_col": 0,
#             "end_col": 1,
#             "reduce": "sum"
#         }
#     ]}

#     rows = [
#         ["column_name", ""],
#         [ 21, 21 ]
#     ]
#     loader = Loader(manifest, rows)
#     assert(loader.get_string() == "column_sums to: 42")

# def test_group_reduce_by_multiple_to_string():
#     # stuff
#     rows = [
#         [6, 7]
#     ]
