import datetime
import os
import time

import psutil

import additional
from additional import config
from additional import update_checkpoint
from additional import send_log_success_update, send_log_fail_update, send_log_empty_update

counter_process_start = time.perf_counter()
pid = os.getpid()
process_info = psutil.Process(pid)

database_source = config["DB_SOURCE"]["DB_NAME"]
schema_source = "dbo"
table_source = "{table_name}"
table_type = 0
bucket_path = config["DB_DESTINATION"]["BUCKET_PATH"]
etl_operation = "update"
checkpoint_type = "last_checkpoint"
get_year = datetime.datetime.now().strftime("%Y")
get_month = datetime.datetime.now().strftime("%m")
get_date = datetime.datetime.now().strftime("%d")

try:
    os.system(
        f"gsutil cp gs://{bucket_path}/{schema_source}/{table_source}/{get_year}/{get_month}/{get_date}/{table_source}-checkpoint.txt ."
    )

    with open(f"{table_source}-checkpoint.txt", "r") as f:
        for line in f:
            checkpoint = line
except FileNotFoundError as fnf_error:
    print("Failed to find specific file:", fnf_error)

cpu_usage = psutil.cpu_percent(interval=None, percpu=False)
mem_info = process_info.memory_full_info()
memory_usage = mem_info.uss / 1024 / 1024

try:
    if "No Update" in open(f"{table_source}-checkpoint.txt").read():
        print("There's no need to update the data")
        counter_process_stop = time.perf_counter()
        process_time = counter_process_stop - counter_process_start
        send_log_empty_update(database_source, schema_source, table_source,
                              etl_operation, process_time, cpu_usage,
                              memory_usage, table_type)
    else:
        update_checkpoint(database_source, schema_source, table_source,
                          checkpoint, checkpoint_type)
        counter_process_stop = time.perf_counter()
        process_time = counter_process_stop - counter_process_start
        send_log_success_update(database_source, schema_source, table_source,
                                etl_operation, process_time, cpu_usage,
                                memory_usage, table_type)
except Exception as exception_message:
    print(exception_message)
    counter_process_stop = time.perf_counter()
    process_time = counter_process_stop - counter_process_start
    send_log_fail_update(database_source, schema_source, table_source,
                         etl_operation, process_time, cpu_usage, memory_usage,
                         table_type)
