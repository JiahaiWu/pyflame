import glob
import re
import pandas as pd


class SparkUDTFTester:

    def __init__(self):
        self.res_df = None
        self.__df = None

    @staticmethod
    def csv_repair(str):
        return re.sub(r'\\"?', '"', str).strip('"')

    def load_csv_to_pandas(self, csvpath, inject_funcs, header=0, sep='\t'):
        dataset = []
        for csv_path in glob.glob(csvpath):
            print(csv_path)
            df = pd.read_csv(csv_path, header=header, sep=sep)
            dataset.append(df)
        cdf = pd.concat(dataset, axis=0, ignore_index=True)
        if inject_funcs is not None:
            cdf = inject_funcs(cdf)
        self.__df = cdf
        return self

    def sort_within_partitions(self, *asc_keys, **kwargs):
        for key in asc_keys:
            self.__df.sort_values(by=key, ascending=True, inplace=True)
        for key, sort_tag in kwargs.items():
            self.__df.sort_values(by=key, ascending=True if sort_tag.Lower() == "asc" else False, inplace=True)
        return self

    def map_partitions(self, udtf_clazz):
        res_rows = list(iter(udtf_clazz(self.__df)))
        self.res_df = pd.DataFrame(res_rows, columns=udtf_clazz.get_schema().names)
        return self

    def to_csv(self, dump_path):
        assert self.res_df is not None, "self.res_df not invalid!!!"
        self.res_df.to_csv(dump_path, sep='\t', header=True, index=False)
        return self.res_df

    def get_df(self):
        assert self.__df is not None, "self._df not invalid!!!"
        return self.__df
