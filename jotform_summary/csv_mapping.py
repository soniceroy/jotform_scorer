from pydantic import BaseModel, Field
from typing import Union, Literal, Optional, Dict
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

class ScalarLoadingDescription(BaseModel):
    load_type: Literal["scalar"]
    col_num: int
    label: Optional[
        Union[
            Literal["from_col"],
            str
        ]
    ] = None
    label_suffix: Optional[str]
    row_num: int
    ignore_if_empty_string: bool = False
    map: Optional[Union[
        StringMapping,
        str
    ]] = None

class PreloadDescription(BaseModel):
    col_num: int
    row_num: int
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
    label_suffix: Optional[str]
    row_num: int
    cols: Union[list[int], ColumnRange]
    reduce: Union[
        Literal["sum"],
        Literal["average"],
        Literal["multiple"],
        ReduceSumThenMultiplyBy,
        ReduceAverageThenMultiplyBy
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
            if type(description.map) == BinaryMapping:
                self.map_binary(description)
            if type(description.map) == RangeMapping:
                self.map_range(description)
    
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
            skip = self.map_row_to_output(loading_description)
            if skip:
                continue
            self._output += '\n'

    def map_row_to_output(self, loading_description):
        if type(loading_description) == ScalarLoadingDescription:
            return self.map_scalar_row_to_output(loading_description)
        elif type(loading_description) == GroupLoadingDescription:
            return self.reduce_group_to_output(loading_description)
    
    def reduce_group_to_output(self, loading_description):
        group = self.get_group(loading_description)
        reduction = 0.0
        if loading_description.reduce == 'sum':
            reduction = reduce(lambda x, y: float(x) + y, group)
        elif loading_description.reduce == 'multiple':
            reduction = reduce(lambda x, y: float(x) * y, group)
        elif loading_description.reduce == 'average':
            reduction = reduce(lambda x, y: float(x) + y, group) / len(group)
        elif type(loading_description.reduce) == ReduceSumThenMultiplyBy:
            reduction = reduce(lambda x, y: float(x) + y, group)
            reduction *= loading_description.reduce.sum_then_multiply_by
        elif type(loading_description.reduce) == ReduceAverageThenMultiplyBy:
            reduction = reduce(lambda x, y: float(x) + y, group) / len(group)
            reduction *= loading_description.reduce.average_then_multiply_by
        self._output = loading_description.label
        if loading_description.label_suffix is not None:
            self._output += loading_description.label_suffix
        self._output += str(reduction)

    def get_group(self, loading_description):
        if type(loading_description.cols) == ColumnRange:
            start = loading_description.cols.start
            end = loading_description.cols.end + 1
            return self.rows[loading_description.row_num][start:end]
        
        # else it is a non-contiguous group
        return [
            self.rows[loading_description.row_num][col] for col in loading_description.cols
        ]

    def map_scalar_row_to_output(self, loading_description: ScalarLoadingDescription):
        if loading_description.ignore_if_empty_string:
            val = self.rows[loading_description.row_num][loading_description.col_num]
            if val == "":
                return True
        header_row = self.rows[0]
        if loading_description.map is None:
            self._output = header_row[loading_description.col_num]
            if loading_description.label_suffix is not None:
                self._output += loading_description.label_suffix
                return
        elif type(loading_description.map) == StringMapping:
            self._output = loading_description.map.output
        else:
            raise LoaderException("Could not map")
        
        if loading_description.label is None:
            return

        if loading_description.label_suffix is not None:
            self._output = loading_description.label + loading_description.label_suffix + self._output
        else:
            self._output = loading_description.label + self._output
    
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