import json
import pytest
from jotform_summary.csv_mapping import Loader, RangeMappingException

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
    assert(loader.get_string() == "to you\n")

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
    assert(loader.get_string() == "happy birthday to you\n")


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
    assert(loader.get_string() == "happy birthday to you\n")


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
    assert(loader.get_string() == "happy birthday to you\n")

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
    assert(loader.get_string() == "happy birthday to you\n")

def test_ignore_if_empty_string_value_in_scalar():
    manifest = {"cargo": [
        {
            "load_type": "scalar",
            "label": "Comments: ",
            "col_num": 1,
            "row_num": 1,
            "ignore_if_empty_string": True
        }
    ]}
    rows = [
        ["dang", "happy birthday"],
        ["on it", ""]
    ]
    loader = Loader(manifest, rows)
    assert(loader.get_string() == "")

def test_group_reduce_by_sum_to_string_with_label():
    manifest = {"cargo": [
        {
            "load_type": "group",
            "label": "group sums to: ",
            "row_num": 1,
            "cols": {"start": 0, "end": 1},
            "reduce": "sum"
        }
    ]}

    rows = [
        ["column_name", ""],
        [ 21, 21 ]
    ]
    loader = Loader(manifest, rows)
    assert(loader.get_string() == "group sums to: 42.0\n")

def test_group_reduce_by_multiple_to_string():
    manifest = {"cargo": [
        {
            "load_type": "group",
            "label": "group multiplies to: ",
            "row_num": 1,
            "cols": {"start": 0, "end": 1},
            "reduce": "multiple"
        }
    ]}

    rows = [
        ["column_name", ""],
        [ 6, 7 ]
    ]
    loader = Loader(manifest, rows)
    assert(loader.get_string() == "group multiplies to: 42.0\n")

def test_group_reduce_by_average_to_string():
    manifest = {"cargo": [
        {
            "load_type": "group",
            "label": "group averages to: ",
            "row_num": 1,
            "cols": {"start": 0, "end": 1},
            "reduce": "average"
        }
    ]}

    rows = [
        ["column_name", ""],
        [ 21, 63 ]
    ]
    loader = Loader(manifest, rows)
    assert(loader.get_string() == "group averages to: 42.0\n")


def test_group_reduce_by_sum_then_multiple_to_string():
    manifest = {"cargo": [
        {
            "load_type": "group",
            "label": "group sums then multiplies to: ",
            "row_num": 1,
            "cols": {"start": 0, "end": 1},
            "reduce": {"sum_then_multiply_by": 20}
        }
    ]}

    rows = [
        ["column_name", ""],
        [ 11, 10 ]
    ]
    loader = Loader(manifest, rows)
    assert(loader.get_string() == "group sums then multiplies to: 420.0\n")

# TODO: test_group_reduce_by_percentage_to_string (average then times 100)

def test_group_reduce_by_average_then_multiple_to_string():
    manifest = {"cargo": [
        {
            "load_type": "group",
            "label": "group averages then multiplies to: ",
            "row_num": 1,
            "cols": [0,2],
            "reduce": {"average_then_multiply_by": 10}
        }
    ]}

    rows = [
        ["column_name", "", ""],
        [ 21, 5, 63 ]
    ]
    loader = Loader(manifest, rows)
    assert(loader.get_string() == "group averages then multiplies to: 420.0\n")

def test_non_contiguous_group_by_average_to_string():
    manifest = {"cargo": [
        {
            "load_type": "group",
            "label": "group averages to: ",
            "row_num": 1,
            "cols": [0,2],
            "reduce": "average"
        }
    ]}

    rows = [
        ["column_name", "", ""],
        [ 21, 5, 63 ]
    ]
    loader = Loader(manifest, rows)
    assert(loader.get_string() == "group averages to: 42.0\n")

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

def test_preload_text_to_binary_scalar_map_to_one_with_col_num_as_ranges():
    manifest = {"preload": [
        {
            "col_num": {"ranges": [[0,1]]},
            "row_num": 1,
            "map": {"map_type": "binary", "is_one": "true"}
        }],
        "cargo": []
    }
    rows = [
        ["dang", "happy birthday"],
        ["true", "true"]
    ]
    loader = Loader(manifest, rows)
    assert(all(map(lambda x: x == 1, loader.rows[1])))


def test_preload_binary():
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

def test_preload_range():
    manifest = {"preload": [
        {
            "col_num": 0,
            "row_num": 1,
            "map": {
                "map_type": "range", 
                "range_map": {
                    "zero": 0,
                    "one": 1,
                    "two": 2
                }
            }
        }],
        "cargo": []
    }
    rows = [
        ["dang", "happy birthday"],
        ["zero", "nope"]
    ]
    loader = Loader(manifest, rows)
    assert(loader.rows[1][0] == 0)

def test_preload_range_errors_when_out_of_range():
    manifest = {"preload": [
        {
            "col_num": 0,
            "row_num": 1,
            "map": {
                "map_type": "range", 
                "range_map": {
                    "zero": 0,
                    "one": 1,
                    "two": 2
                }
            }
        }],
        "cargo": []
    }
    rows = [
        ["dang", "happy birthday"],
        ["not zero thru two", "nope"]
    ]
    with pytest.raises(RangeMappingException):
        loader = Loader(manifest, rows)
