import datetime
import os
import time

from google.cloud import bigquery
import psutil

import additional
from additional import config
from additional import send_log_success_load, send_log_fail_load, send_log_empty_load

counter_process_start = time.perf_counter()
pid = os.getpid()
process_info = psutil.Process(pid)

project_id = config["CLOUD"]["PROJECT_ID"]
database_source = config["DB_SOURCE"]["DB_NAME"]
staging_dataset_id = "STAGING_{dataset_name}"
dataset_id = "{dataset_name}"
schema_source = "dbo"
table_source = "{table_name}"
table_id = "{table_name}"
table_type = 0
complete_staging_table_id = f"{project_id}.{staging_dataset_id}.{table_id}"
complete_table_id = f"{project_id}.{dataset_id}.{table_id}"
bucket_path = config["DB_DESTINATION"]["BUCKET_PATH"]
etl_operation = "load"
get_year = datetime.datetime.now().strftime("%Y")
get_month = datetime.datetime.now().strftime("%m")
get_date = datetime.datetime.now().strftime("%d")


def load_to_staging():
    try:
        client = bigquery.Client()
        table_partition = bigquery.table.TimePartitioning(field="modified_date")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=table_partition,
        )

        uri = f"gs://{bucket_path}/{schema_source}/{table_source}/{get_year}/{get_month}/{get_date}/*.parquet"
        load_job = client.load_table_from_uri(uri,
                                              complete_staging_table_id,
                                              job_config=job_config)

        load_job.result()

        destination_table = client.get_table(complete_staging_table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))
    except Exception as exception_message:
        print("Failed to load new data into staging datasets:",
              exception_message)


def merge_in_bq():
    try:
        client = bigquery.Client()
        query = """MERGE INTO {dataset_name}.{table_name} scn
                USING STAGING_{dataset_name}.{table_name} sscn
                ON scn.{primary_key_col} = sscn.{primary_key_col}
                WHEN MATCHED THEN DELETE
                """
        query_job = client.query(query, location="asia-southeast2")
        query_job.result()
    except Exception as exception_message:
        print("Failed to merge new data:", exception_message)


def delete_staging():
    try:
        client = bigquery.Client(location="asia-southeast2")
        client.delete_table(complete_staging_table_id, not_found_ok=True)
        print(f"Deleted table '{complete_staging_table_id}'.")
    except Exception as exception_message:
        print("Failed to delete staging table:", exception_message)


def load_to_bq():
    try:
        client = bigquery.Client()
        table_partition = bigquery.table.TimePartitioning(field="modified_date")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            time_partitioning=table_partition,
        )

        uri = f"gs://{bucket_path}/{schema_source}/{table_source}/{get_year}/{get_month}/{get_date}/*.parquet"
        load_job = client.load_table_from_uri(uri,
                                              complete_table_id,
                                              job_config=job_config)

        load_job.result()

        destination_table = client.get_table(complete_table_id)
        print("Total {} rows.".format(destination_table.num_rows))
    except Exception as exception_message:
        print("Failed to load new data:", exception_message)


try:
    os.system(
        f"gsutil cp gs://{bucket_path}/{schema_source}/{table_source}/$(date +%Y)/$(date +%m)/$(date +%d)/{table_source}-checkpoint.txt ."
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
        print("There's no new data to be load")
        counter_process_stop = time.perf_counter()
        process_time = counter_process_stop - counter_process_start
        send_log_empty_load(database_source, schema_source, table_source,
                            etl_operation, process_time, cpu_usage,
                            memory_usage, table_type)
    else:
        load_to_staging()
        merge_in_bq()
        delete_staging()
        load_to_bq()
        counter_process_stop = time.perf_counter()
        process_time = counter_process_stop - counter_process_start
        send_log_success_load(database_source, schema_source, table_source,
                              etl_operation, process_time, cpu_usage,
                              memory_usage, table_type)
except Exception as exception_message:
    print(exception_message)
    counter_process_stop = time.perf_counter()
    process_time = counter_process_stop - counter_process_start
    send_log_fail_load(database_source, schema_source, table_source,
                       etl_operation, process_time, cpu_usage, memory_usage,
                       table_type)
