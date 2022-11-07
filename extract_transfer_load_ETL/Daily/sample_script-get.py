import datetime
import json
import os
import time

import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import isnan, when, count, col
import psutil

import additional
from additional import config
from additional import retrieve_checkpoint, write_new_checkpoint, write_none_checkpoint
from additional import send_log_success_get, send_log_fail_get, send_log_empty_get
from additional import delete_gcs_path

counter_process_start = time.perf_counter()
pid = os.getpid()
process_info = psutil.Process(pid)

conf = SparkConf()
conf.setAppName("Sync-{table_name}-Get")
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)

database_source = config["DB_SOURCE"]["DB_NAME"]
schema_source = "dbo"
table_source = "{table_name}"
table_type = 0
user_source = config["DB_SOURCE"]["DB_USER"]
password_source = config["DB_SOURCE"]["DB_PWD"]
database_host = config["DB_SOURCE"]["DB_HOST"]
database_port = config["DB_SOURCE"]["DB_PORT"]
bucket_path = config["DB_DESTINATION"]["BUCKET_PATH"]
driver_jdbc = config["MISC"]["DRIVER_MSSQL"]
etl_operation = "get"
checkpoint_type = "last_checkpoint"
last_checkpoint = retrieve_checkpoint(database_source, schema_source,
                                      table_source, checkpoint_type)
print(last_checkpoint)
get_year = datetime.datetime.now().strftime("%Y")
get_month = datetime.datetime.now().strftime("%m")
get_date = datetime.datetime.now().strftime("%d")
delete_gcs_path(schema_source, table_source, get_year, get_month, get_date)

sql =   "(SELECT column_1, column_2, column_3, modified_date "\
        f"FROM {table_source} WHERE CAST(modified_date AS DATETIME2(7)) > '{last_checkpoint}') t1"

try:
    df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://{database_host};database={database_source}") \
        .option("dbtable", sql) \
        .option("user", user_source) \
        .option("password", password_source) \
        .option("driver", driver_jdbc) \
        .load()

    df_checkpoint = df.agg({"modified_date": "max"}).collect()[0][0]
    df_row_count = df.count()

except Exception as exception_message:
    print("Error when getting new data:", exception_message)

cpu_usage = psutil.cpu_percent(interval=None, percpu=False)
mem_info = process_info.memory_full_info()
memory_usage = mem_info.uss / 1024 / 1024

try:
    if df_checkpoint is None:
        write_none_checkpoint(schema_source, table_source, bucket_path)
        counter_process_stop = time.perf_counter()
        process_time = counter_process_stop - counter_process_start
        send_log_empty_get(database_source, schema_source, table_source,
                           etl_operation, process_time, df_row_count, cpu_usage,
                           memory_usage, table_type)
    else:
        counter_write_start = time.perf_counter()
        df.write.parquet(
            f"gs://{bucket_path}/{schema_source}/{table_source}/{get_year}/{get_month}/{get_date}"
        )
        counter_write_stop = time.perf_counter()
        counter_write_time = counter_write_stop - counter_write_start
        write_new_checkpoint(schema_source, table_source, bucket_path,
                             df_checkpoint)
        counter_process_stop = time.perf_counter()
        process_time = counter_process_stop - counter_process_start
        send_log_success_get(database_source, schema_source, table_source,
                             etl_operation, process_time, df_row_count,
                             cpu_usage, memory_usage, table_type)
except Exception as exception_message:
    print(exception_message)
    counter_process_stop = time.perf_counter()
    process_time = counter_process_stop - counter_process_start
    send_log_fail_get(database_source, schema_source, table_source,
                      etl_operation, process_time, df_row_count, cpu_usage,
                      memory_usage, table_type)
