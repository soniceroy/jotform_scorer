from pydantic import BaseModel
from typing import Union, Literal, Optional


class Prefixing(BaseModel):
    starts_with: str


class StringMapping(BaseModel):
    map_type: Literal["string"]
    output: str


class ScalarLoadingDescription(BaseModel):
    column_name: Union[Prefixing, str]
    row_num: int
    map: Optional[Union[
        StringMapping,
        str
    ]] # None means we will output column_name directly

class LoaderException(Exception):
    pass


class Loader:
    def __init__(self, manifest: list, rows: list):
        self.manifest = [ScalarLoadingDescription(**item) for item in manifest] 
        self.rows = rows
        self._output = ""
        self.map_rows_to_output()

    def get_string(self):
        return self._output
    
    def map_rows_to_output(self):
        for loading_description in self.manifest:
            return self.map_row_to_output(loading_description)
    
    def map_row_to_output(self, loading_description):
        column_index = -1
        header_row = self.rows[0] 
        if type(loading_description.column_name) == str:
            column_index = header_row.index(loading_description.column_name)
        if type(loading_description.column_name) == Prefixing:
            prefix = loading_description.column_name.starts_with      
            for i, column_name in enumerate(header_row):
                if column_name.startswith(prefix):
                    column_index = i
        if column_index == -1:
            raise LoaderException("Prefix not found")
        if loading_description.map is None:
            self._output = header_row[column_index]
        elif type(loading_description.map) == StringMapping:
            self._output = loading_description.map.output
        else:
            raise LoaderException("Could not map")

