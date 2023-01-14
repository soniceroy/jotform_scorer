from pydantic import BaseModel, Field
from typing import Union, Literal, Optional
from typing_extensions import Annotated

class Prefixing(BaseModel):
    starts_with: str


class StringMapping(BaseModel):
    map_type: Literal["static_string"]
    output: str

class ColumnNumber(BaseModel):
    col_num: int


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
    map: Optional[Union[
        StringMapping,
        str
    ]] = None



class GroupLoadingDescription(BaseModel):
    load_type: Literal["group"]
    label: Union[
        str,
        ColumnNumber
    ]
    label_suffix: Optional[str]
    row_num: int
    start_col: int
    end_col: int
    reduce: Union[
        Literal["sum"],
        Literal["average"],
        Literal["multiple"]
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

class LoaderException(Exception):
    pass




class Loader:
    def __init__(self, manifest: dict, rows: list):
        self.manifest = Manifest(**manifest).cargo
        self.rows = rows
        self._output = ""
        self.map_rows_to_output()

    def get_string(self):
        return self._output
    
    def map_rows_to_output(self):
        for loading_description in self.manifest:
            return self.map_row_to_output(loading_description)
    
    def map_row_to_output(self, loading_description):
        if type(loading_description) == ScalarLoadingDescription:
            return self.map_scalar_row_to_output(loading_description)
        elif type(loading_description) == GroupLoadingDescription:
            return self.reduce_group_to_output(loading_description)
    
    def reduce_group_to_output(self, loading_description):
        header_row = self.rows[0]
        column_index = self.get_column_index(loading_description, header_row)


    def map_scalar_row_to_output(self, loading_description):
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