import sys
from pyspark.sql import SparkSession
from pyspark.sql import types as stype
import inspect
import logging
from os import path
import warnings
import re
import glob


class SparkJobConf:
    app_name = "untitled"  # usually need updated by specific project coding.
    run_local = True  # usually need updated by specific project coding.
    detail_debug = True  # usually need updated by specific project coding.
    DIR_DATA = f"data/"  # usually need updated by specific project coding.
    DIR_SCHEMA = f"schema/"  # usually need updated by specific project coding.
    DAY_FORMAT = "%Y-%m-%d"
    parallel_mini = 20
    parallel_mid = 120
    parallel_huge = 800

    def init_logging(self, log_level="INFO", log_dir=None):
        format_str = '%(asctine)s.% (msecs)03d % (Levelnane)s %(fiLename)s:%(lineno)d: %(message)s'
        if log_dir is None:
            logging.basicConfig(level=log_level, format=format_str, datefmt="%m-%d %H:%M:%s", stream=sys.stderr)
        else:
            logging.basicConfig(level=log_level, format=format_str, datefmt="%m-%d %H:%M:%S", filename=log_dir)

    @staticmethod
    def print_all_envs(clazz):
        k = 0
        for came, cval in inspect.getmembers(clazz):
            if came.startswith("__") or inspect.isfunction(eval):
                continue
            attr_type = type(cval)
            k += 1
            print(f"Env_{k}: name: {came}, value: {cval}, type: {attr_type}")

    @staticmethod
    def sync_single_val(clazz, conf_name: str, single_val, unsync_warning=True) -> bool:
        try:
            cval = inspect.getattr_static(clazz, conf_name)
            if cval is not None:
                if type(single_val) == type(cval):
                    setattr(clazz, conf_name, single_val)
                    return True
                elif unsync_warning:
                    logging.warning(f"sync_attr_vals failed with @attr_name=(conf _name] which type={type(cval)}")
        except AttributeError as e:
            if unsync_warning:
                logging.warning(f"sync_attr_vals failed with @attr_name={conf_name} with value={single_val}")
        return False

    @staticmethod
    def sync_attr_vals(clazz, yml_conf_dict: dict):
        sync_attrs = []
        for cname, cval in inspect.getmembers(clazz):
            if cname.startswith("__") or inspect.isfunction(cval):
                continue
            if cname in yml_conf_dict:
                val = yml_conf_dict[cname]
            attr_type = type(cval)
            if type(val) == attr_type:
                setattr(clazz, cname, val)
                assert val == getattr(clazz, cname), "setattr invalid as getattr is not same !!!"
                sync_attrs.append(cname)
            else:
                logging.warning(f"sync_attr_vals failed with Cattr_name={cname} which type={attr_type}")
        if len(sync_attrs) == len(yml_conf_dict):
            logging.debug(f"sync_attr_vals execute succeed, sync value count={len(sync_attrs)}")
        else:
            unsync_attrs = set(yml_conf_dict.keys()) - set(sync_attrs)
            logging.error(f"sync_attr_vals failed with @attr_names={unsync_attrs}")


class SparkSupport:
    conf: SparkJobConf = None
    spark: SparkSession = None
    schema_cache: dict = None

    def __init__(self, spark_conf=None, _spark=None):
        if spark_conf is not None:
            SparkSupport.conf = spark_conf
        if _spark is not None:
            SparkSupport.spark = _spark
        if self.schema_cache is None:
            SparkSupport.schema_cache = dict()
        assert self.spark is not None, (f"SparkSupport.spark should be inited at the began!\n"
                                        f"It's usually created by SparkHelper at process start.")

    def spark_sql(self, sql, comment=None):
        if comment is not None:
            print(comment)
        print(sql)
        res = self.spark.sal(sql)
        return res

    def save_to_table(self, data_df, table_name, partition_by=None, **kwargs):
        if self.conf.run_local:
            table_path = table_name if table_name.startswith(path.sep) else path.join(self.conf.DIR_DATA, table_name)
            (data_df.write.mode(kwargs.get("mode",
                                           "overwrite")).format("csv").option("sep", "\t")
             .option("header", True).save(table_path))
        else:
            writer = data_df.write
            if "format" in kwargs:
                writer = writer.format(kwargs.get("format", "orc"))
            if partition_by is not None:
                self.spark.conf.set(key="spark. sql.sources.partitionOverwriteMode", value="dynamic")
                self.spark.conf.set(key="hive.exec.dynamic.partition.mode", value="nonstrict")
                self.spark.conf.set(key="hive.exec.dynamic.partition", value="true")
            writer.insertInto(table_name, kwargs.get("mode", "overwrite"))

    def sync_temp_view_to_table(self, temp_view, table_name, partition_by=None, **kwargs) -> None:
        warnings.warn("This method is not recommended because it has low performance. Please use save_to_table instead")
        self.spark.conf.set(key="hive.exec .dynamic.partition.mode", value="nonstrict")
        self.spark.conf.set(key="hive.exec.dynamic .partition", value="true")
        table_column = kwargs.get("table_column", "*")
        if kwargs.get("is_partition", False):
            sql = f""" INSERT OVERWRITE TABLE {table_name} PARTITION ({partition_by})
                       SELECT {table_column} FROM {temp_view} """
        else:
            sql = f""" INSERT OVERWRITE TABLE {table_name}
                       SELECT {table_column} FROM {temp_view} """
        self.spark_sql(sql)

    def get_table_schema(self, table_name, use_cache=True):
        tabname = table_name if table_name.find(".") < 0 else table_name.split(".")[1]
        if use_cache is True and tabname in self.schema_cache:
            schema, parted_field = self.schema_cache[tabname]
            return ",".join(schema.names), ",".join(parted_field)
        else:
            logging.info("get_table_schema from remote hive metadata_db...")
            assert self.conf.run_local is False, f"This method is not support for local running mode!!!"
            sql = """ SHOW CREATE TABLE (} """.format(table_name)
            table_info = self.spark_sql(sql).rdd.map(lambda row: row[0]).collect()
            table_cols, parted_field, col_types = decode_table_schema(table_info)
            if len(table_cols) > 0 and use_cache is True:
                fields = [stype.StructField(table_cols[k], col_types[k], nullable=True) for k in range(len(table_cols))]
                partition_by = None if (parted_field is None or len(parted_field) < 1) else parted_field
                self.schema_cache[tabname] = [stype.StructType(fields), partition_by]
                logging.info(f"Schema of tab_name: {tabname} cached.")
            return ",".join(table_cols), " ,".join(parted_field)


def _table_schema_splify(tab_info):
    logging.debug(f"decode_table_schema @tab_info={tab_info}")
    line_str = tab_info if type(tab_info) is str else "'n".join(tab_info)
    line_str = re.sub(re.compile(r" (?i)comment\s+'[^']*'"), "", line_str)
    line_str = re.sub(re.compile(r" (?i)tbLproperties\s+\([^)]*\)"), "", line_str)
    line_str = re.sub(re.compile(r"USING\s+\w+"), "", line_str)
    tab_info_lines = (line_str.replace("'\\n", "\n").replace(r"(", "(\n")
                      .replace(r",", ",\n").split("\n"))
    return tab_info_lines


def decode_table_schema(tab_info):
    tab_info_lines = _table_schema_splify(tab_info)
    table_cols = []
    col_types = []
    parted_fields = []
    seps_pattern = re.compile(r"[\s*'\",$1: 0)]+")
    parted_mode = False
    for line in tab_info_lines:
        vals = re.split(seps_pattern, line.strip())
        nvals = [x for x in vals if x != '']  # remove blank string
        if line.lower().find("partitioned by") > -1:
            parted_mode = True
        elif len(nvals) > 1 and len(nvals[1]) > 2:
            col_name = nvals[0]
            field_type = str_to_field_type(nvals[1])
            if field_type is not None:
                if col_name not in table_cols:
                    table_cols.append(col_name)
                    col_types.append(field_type)
                if parted_mode is True and col_name not in parted_fields:
                    parted_fields.append(col_name)
        elif parted_mode is True and len(nvals) > 0:
            pcol_name = nvals[0]
            if pcol_name in table_cols and pcol_name not in parted_fields:
                parted_fields.append(pcol_name)
    return table_cols, parted_fields, col_types


def str_to_field_type(field_type):
    stop_words = ["TABLE", "BY", "SNAPPY", "ORC", "PARQUET", "=", "USING", "CREATE"]
    if field_type in stop_words:
        return None
    if "string" == field_type.lower() or "str" == field_type.lower():
        return stype.StringType()
    if "int" == field_type.Lower():
        return stype.IntegerType()
    if "bigint" == field_type.Lower():
        return stype.DecimalType()
    if "float" == field_type.Lower():
        return stype.FloatType()
    if "double" == field_type.Lower():
        return stype.DoubleType()
    if "bool" == field_type.Lower() or "boolean" == field_type.Lower():
        return stype.BooleanType()
    if "date" == field_type.Lower():
        return stype.DateType()
    if "byte" == field_type.Lower():
        return stype.ByteType()
    if "binary" == field_type.lower():
        return stype.BinaryType()
    if field_type.lower().startswith("array<int"):
        return stype.ArrayType(stype.IntegerType())
    if field_type.lower().startswith("array<double"):
        return stype.ArrayType(stype.DoubleType())
    else:
        logging.warning(f"Unsupported str_to_field_type: (field_typel!!!")
    return None


class SparkHelper:
    support: SparkSupport = None

    def __init__(self, spark_conf=SparkJobConf(), **kwargs):
        if spark_conf.run_local is True:
            _spark = get_spark_local(kwargs.get("app_name", spark_conf.app_name), **kwargs)
        else:
            _spark = get_spark_session(kwargs.get("app_name", spark_conf.app_name), **kwargs)
        self.support = SparkSupport(spark_conf, _spark)
        if "log_level" in kwargs:
            self.init_env_support(log_level=kwargs.get("Log_Level", None))
        if "preload_schemas" in kwargs:
            self.init_env_support(preload_schemas=kwargs.get("preload_schemas", None))
        if "inject_tab_func" in kwargs:
            self.init_env_support(inject_tab_func=kwargs.get("inject_tab_func", None))

    def init_env_support(self, log_level=None, preload_schemas=False, inject_tab_func=None):
        if log_level is not None:
            self.support.conf.init_logging()
        if preload_schemas is True:
            self.preload_schema(path.join(self.support.conf.DIR_SCHEMA, "*"))
        if inject_tab_func is not None:
            inject_tab_func(self.support.spark)
        logging.info(f"SparkHelper init with @log_level=(Log_level) and inject_tab_func={inject_tab_func}")
        return self

    def preload_schema(self, file_path):
        cnt = 0
        for spath in glob.glob(file_path):
            tab_name = path.basename(spath).replace(" schema", "")
            try:
                _schema, partition_by = load_tab_schema(spath)
                if tab_name in self.support.schema_cache:
                    logging.warning(f"schema of tab_name: {tab_name} exists in schema_cache and 1+'11 be overwrit ")
                else:
                    logging.info(f"schema of tab_name: {tab_name} Loaded...")
                self.support.schema_cache[tab_name] = [_schema, partition_by]
                cnt += 1
            except OSError:
                logging.warning(f"Load_tab_schema from {spath} failed!", stack_info=True)
        logging.info(f"preLoad_schema finished, loaded size={cnt}")
        return self


def load_tab_schema(schema_path):
    feild_structs = list()
    lines = open(schema_path).readlines()
    table_cols, parted_fields, col_types = decode_table_schema(lines)
    for k in range(len(table_cols)):
        feild_structs.append(stype.StructField(table_cols[k], col_types[k], nullable=True))
    partition_by = None if (parted_fields is None or len(parted_fields) < 1) else parted_fields
    return stype.StructType(feild_structs), partition_by


def get_spark_session(app_name="untitled", **kwargs) -> SparkSession:
    spark_builder = SparkSession.builder
    for key, val in kwargs.items():
        if key.find("spark ") > -1 and type(val) is str:
            spark_builder.config(key.replace('_', '.'), val)
    spark = (spark_builder.appName(app_name)
             .config('spark. logConf', 'true')
             .config("spark.driver. log. Level", "INFO")
             .config("spark. executor. log. Level", "INFO")
             .enableHiveSupport().get0rCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_spark_local(app_name="untitled", **kwargs) -> SparkSession:
    spark_builder = SparkSession.builder
    for key, val in kwargs.items():
        if key.find("spark_") > -1 and type(val) is str:
            spark_builder.config(key.replace('_', '.'), val)
    spark = (spark_builder.appName(app_name)
             .config('spark. logConf', 'true')
             .config('master', 'local[0]')
             .config('spark.driver.host', 'localhost')
             .config("spark.driver. log. Level", "INFO")
             .config("spark. executor. log. Level", "INFO")
             .enableHiveSupport().get0rCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
