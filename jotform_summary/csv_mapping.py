from pydantic import BaseModel, Field, conlist
from typing import Union, Literal, Optional, Protocol, Dict
from typing_extensions import Annotated
from functools import reduce

class Prefixing(BaseModel):
    starts_with: str


class StringMapping(BaseModel):
    map_type: Literal["static_string"]
    output: str

class ColumnNumber(BaseModel):
    col_num: int

class BinaryMapping(BaseModel):
    map_type: Literal["binary"]
    is_one: str

class RangeMapping(BaseModel):
    map_type: Literal["range"]
    range_map: Dict[str, int]


class ScalarLoadingDescriptionException(Exception):
    pass

class ScalarLoadingDescription(BaseModel):
    load_type: Literal["scalar"]
    col_num: int
    label: Optional[
        Union[
            Literal["from_col"],
            str
        ]
    ] = None
    label_suffix: str = ""
    row_num: int
    ignore_if_empty_string: bool = False
    map: Optional[Union[
        StringMapping,
        str
    ]] = None

    def output(self, rows):
        if self.ignore_if_empty_string:
            val = rows[self.row_num][self.col_num]
            if val == "":
                return val
        header_row = rows[0]
        out = ""
        if self.map is None:
            return header_row[self.col_num] + self.label_suffix + "\n"
        elif type(self.map) == StringMapping:
            out += self.map.output
        else:
            raise ScalarLoadingDescriptionException("Could not map")
        
        if self.label is None:
            return out + "\n"
        
        if self.label_suffix is not None:
            return self.label + self.label_suffix + out + "\n"


class PreloadRangesAndOneOffs(BaseModel):
    ranges: list[Union[int, conlist(int, min_items=2, max_items=2)]]

class PreloadDescription(BaseModel):
    col_num: Union[PreloadRangesAndOneOffs, int]
    row_num: int = 1
    map: Union[BinaryMapping, RangeMapping]

class ColumnRange(BaseModel):
    start: int
    end: int

class ReduceSumThenMultiplyBy(BaseModel):
    sum_then_multiply_by: int

class ReduceAverageThenMultiplyBy(BaseModel):
    average_then_multiply_by: int

class GroupLoadingDescription(BaseModel):
    load_type: Literal["group"]
    label: Union[
        str,
        ColumnNumber
    ]
    label_suffix: str = ""
    row_num: int = 1
    cols: Union[list[int], ColumnRange]
    reduce: Union[
        Literal["sum"],
        Literal["average"],
        Literal["multiple"],
        ReduceSumThenMultiplyBy,
        ReduceAverageThenMultiplyBy
    ]

    def output(self, rows):
        group = self.get_group(rows)
        reduction = 0.0
        if self.reduce == 'sum':
            reduction = reduce(lambda x, y: float(x) + y, group)
        elif self.reduce == 'multiple':
            reduction = reduce(lambda x, y: float(x) * y, group)
        elif self.reduce == 'average':
            reduction = reduce(lambda x, y: float(x) + y, group) / len(group)
        elif type(self.reduce) == ReduceSumThenMultiplyBy:
            reduction = reduce(lambda x, y: float(x) + y, group)
            reduction *= self.reduce.sum_then_multiply_by
        elif type(self.reduce) == ReduceAverageThenMultiplyBy:
            reduction = reduce(lambda x, y: float(x) + y, group) / len(group)
            reduction *= self.reduce.average_then_multiply_by

        return self.label + self.label_suffix + str(reduction) + "\n"

    def get_group(self, rows):
        if type(self.cols) == ColumnRange:
            start = self.cols.start
            end = self.cols.end + 1
            return rows[self.row_num][start:end]
        # else it is a non-contiguous group
        return [
            rows[self.row_num][col] for col in self.cols
        ]

LoadingDescription = Annotated[
    Union[
        ScalarLoadingDescription, 
        GroupLoadingDescription
    ], 
    Field(discriminator='load_type')
]

class Manifest(BaseModel):
    cargo: list[LoadingDescription]
    preload: list[PreloadDescription] = []

class LoaderException(Exception):
    pass




class Loader:
    def __init__(self, manifest: dict, rows: list):
        manifest = Manifest(**manifest)
        self.cargo = manifest.cargo
        self.preload_descriptions = manifest.preload
        self.rows = rows
        self._output = ""
        self.preload()
        self.map_rows_to_output()

    def get_string(self):
        return self._output
    
    def preload(self):
        for description in self.preload_descriptions:
            if type(description.col_num) == PreloadRangesAndOneOffs:
                for col_num in self.get_columns_from_ranges_and_oneoffs(description.col_num.ranges):
                    new_description = PreloadDescription(
                        col_num=col_num, 
                        row_num=description.row_num,
                        map=description.map
                    )
                    self.preload_map_to_value(new_description)
            else:
                self.preload_map_to_value(description)

    def preload_map_to_value(self, description):
            if type(description.map) == BinaryMapping:
                self.map_binary(description)
            if type(description.map) == RangeMapping:
                self.map_range(description)
       
    def get_columns_from_ranges_and_oneoffs(self, ranges_and_oneoffs: PreloadRangesAndOneOffs):
        for range_or_oneoff in ranges_and_oneoffs:
            if type(range_or_oneoff) == list:
                for i in range(range_or_oneoff[0], range_or_oneoff[1] + 1):
                    yield i
            else:
                yield range_or_oneoff

    def map_range(self, description: RangeMapping):
        value = self.rows[description.row_num][description.col_num]
        range_value = description.map.range_map.get(value, None)
        if range_value is None:
            raise LoaderException(
                f'RangeMappingError: {value} not in range_map {description.map.range_map}'
            )
        self.rows[description.row_num][description.col_num] = range_value

    def map_binary(self, description: BinaryMapping):
        value = self.rows[description.row_num][description.col_num]
        binary_value = int(value == description.map.is_one)
        self.rows[description.row_num][description.col_num] = binary_value

    def map_rows_to_output(self):
        for loading_description in self.cargo:
            self._output += loading_description.output(self.rows)

    def get_column_index(self, loading_description, header_row):
        column_index = -1
        if type(loading_description.column_name) == str:
            column_index = header_row.index(loading_description.column_name)
        if type(loading_description.column_name) == Prefixing:
            prefix = loading_description.column_name.starts_with      
            for i, column_name in enumerate(header_row):
                if column_name.startswith(prefix):
                    column_index = i
        return column_index