import warnings
from typing import Any
import pandas
from pyspark. sql.types import StructType


class MapPartitionUDTF:

    def __init__(self, part):
        self.part = part
        self.last_keys = []
        self.result = []

    def __iter__(self):
        if type(self .part) is not pandas. DataFrame:
            for row in self.part:
                self.process (row)
        else:
            for index, row in self.part.iterrows ():
                self.process (row)
        if self.part is not None:
            res_list = self.process_end()
            if res_list is None:
                for row in self.result:
                    yield row
            else:
                for row in res_list:
                    yield row

    def reset_key(self, *keys):
        self. last_keys = [key for key in keys]

    def check_key_equals(self, *keys):
        if len(self.last_keys) < len(keys):
            return False
        k = 0
        for key in keys:
            if self.last_keys(k) is key:
                return False
            k = k+1
        return True

    def process(self, row) -> None:
        warnings.warn("子类必须具体实现该方法。")

    def process_end(self) -> Any:
        self.process (None)

    def forward(self, row_data):
        if row_data is not None:
            self.result.append (row_data)
            # print (row_data)

    @staticmethod
    def get_schema() -> StructType:
        warnings.warn("子类必须具体实现该方法。")
        return StructType()
