import json
import pytest
from jotform_summary.csv_mapping import Loader

# @pytest.fixture
# def manifest():
#     with open("test_data/manifest.json") as f:
#         data = json.load(f)
#     return data


def test_cell_mapped_to_string_by_prefix():
    manifest = [
        {
            "column_name": {"starts_with": "happy"},
            "row_num": 1,
            "map": {"map_type": "string", "output": "to you"}
        }
    ]
    rows = [
        ["dang", "happy birthday"],
        ["on it", "to you"]
    ]
    loader = Loader(manifest, rows)
    assert(loader.get_string() == "to you")

def test_cell_mapped_to_string_by_string():
    manifest = [
        {
            "column_name": "happy birthday",
            "row_num": 1,
            "map": {"map_type": "string", "output": "to you"}
        }
    ]
    rows = [
        ["dang", "happy birthday"],
        ["on it", "to you"]
    ]
    loader = Loader(manifest, rows)
    assert(loader.get_string() == "to you")

def test_group_reduce_by_sum_to_string():
    manifest = [
        {
            "group": {
                "label": "my group: ",
                "row": 0,
                "start_col": 0,
                "end_col": 1,
                "reduce": "sum"
            }
        }
    ]

    rows = [
        [ 11, 11 ]
    ]
    #loader = Loader(manifest, rows)
    #assert(loader.get_string() == 42)

def test_group_reduce_by_multiple_to_string():
    # stuff
    rows = [
        [6, 7]
    ]
