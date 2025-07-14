import os.path
from pyspark.sql.types import *
import pandas as pd
import re
import logging
from flame.spark_env.spark_helper import str_to_field_type, decode_table_schema


def transfer_pure_csv(csv_path, name_list=[], suffix="_new", sep='\t'):
    '''
    读取大数据平台下载的无头标识的CSV类文件，增补列名信息
    :param csv_path:
    param name_list:
    :param suffix:
    :param sep:
    return:
    '''
    data = pd.read_csv (csv_path, encoding='utf-8', sep=sep, names=name_list)
    update_path = csv_path.replace (" csv", suffix + " csv")
    data.to_csv(update_path, encoding='utf-8', sep=sep, index=False)
    return update_path


def load_tab_schema (schema_path) :
    feild_structs = list()
    lines = open (schema_path) .readlines ()
    table_cols, parted_fields, col_types = decode_table_schema (lines)
    for k in range(len(table_cols)):
        feild_structs.append(StructField(table_cols[k], col_types [k], nullable=True))
    partition_by = None if (parted_fields is None or len(parted_fields) < 1) else parted_fields
    return StructType(feild_structs), partition_by


def spark_load_data(spark, csv_path="xxx.csv", schema=None, sep="\t", format="csv"):
    _schema = StructType([
        StructField("IDGC", StringType() , nullable=True),
        StructField("SEARCHNAME", StringType() , nullable=True),
        StructField("PRICE", DoubleType(), nullable=True)
    ])
    if schema is not None:
        if type(schema) is str:
            _schema, partition_by = load_tab_schema(schema)
        else:
            _schema = schema
    sdf = spark.read.Load(csv_path, format=format, header="false", sep=sep, schema=_schema, head=False,
                          inferSchema=True if _schema is None else False)
    return sdf


def load_csv_with_pandas(spark, csv_path, schema=None, sep="\t", skiprows=0):
    '''
    读取本地表格类型的文件，转码生成 spark_dataframe对象实例
    sparam spark:
    :param csv_path:
    :param schema:
    :param sep:
    :param skiprows:
    :return:
    '''
    _schema = StructType([
        StructField("example", StringType(), nullable=True),
    ])
    if schema is not None:
        if type (schema) is str:
            _schema, partition_by = load_tab_schema(schema)
        else:
            _schema = schema
        data = pd.read_csv(csv_path, header=None, encoding='utf-8', sep=sep, names=_schema.names, skiprows=skiprows)
        sdf = spark.createDataFrame(data=data, schema=_schema)
    else:
        data = pd.read_csv(csv_path, header=skiprows, skiprows=skiprows, encoding='utf-8', sep=sep)
        sdf = spark. createDataFrame(data=data)
    return sdf


def inject_table_view(spark, view_name, csv_path, schema_path=None, sep="\t"):
    schema = schema_path if schema_path is not None else csv_path.replace (" csv", " schema")
    if not os.path. exists(schema) :
        schema = None
    spark_df = load_csv_with_pandas (spark, csv_path, schema, sep)
    temp_view = spark_df. createOrReplaceTempView (view_name)
    print(f"create0rReplaceTempView: (view_name]")
    spark_df. show (truncate=False)
    return temp_view
