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

    def get(self, key):
        return int(key == self.is_one)
        
class RangeMappingException(Exception):
    pass

class RangeMapping(BaseModel):
    map_type: Literal["range"]
    range_map: Dict[str, int]

    def get(self, key):
        if key not in self.range_map:
            raise RangeMappingException(f"{key} not in range_map: {self.range_map}")
        return self.range_map[key]


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

    def generate_columns(self):
        for range_or_oneoff in self.ranges:
            if type(range_or_oneoff) == list:
                for i in range(range_or_oneoff[0], range_or_oneoff[1] + 1):
                    yield i
            else:
                yield range_or_oneoff


class PreloadDescription(BaseModel):
    col_num: Union[PreloadRangesAndOneOffs, int]
    row_num: int = 1
    map: Union[BinaryMapping, RangeMapping]

    def preload(self, rows):
        if type(self.col_num) == int:
            columns = [self.col_num]
        else:
            columns = self.col_num.generate_columns()

        for col_num in columns:
            key = rows[self.row_num][col_num]
            value = self.map.get(key)
            rows[self.row_num][col_num] = value


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


class DataMapperProtocol(Protocol):
    def output(rows: list) -> str:
        ...

class PreloadProtocol(Protocol):
    def preload(rows: list) -> None:
        ...

class Loader:
    def __init__(self, manifest: dict, rows: list):
        manifest = Manifest(**manifest)
        self.cargo: list[DataMapperProtocol] = manifest.cargo
        self.preload_descriptions: list[PreloadProtocol] = manifest.preload
        self.rows = rows
        self._output = ""
        self.preload()
        self.map_rows_to_output()

    def get_string(self):
        return self._output
    
    def preload(self):
        for preloader in self.preload_descriptions:
            preloader.preload(self.rows)

    def map_rows_to_output(self):
        for loading_description in self.cargo:
            self._output += loading_description.output(self.rows)
