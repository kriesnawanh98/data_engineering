import logging
import os
import sys

import google.cloud.logging
import psycopg2
from google.cloud import storage

client = google.cloud.logging.Client()
client.get_default_handler()
logging_client = client.setup_logging()
logger_name = "{log_name}"
log_version = "1.2.3"
logger = client.logger(logger_name)

config = {
    "CLOUD": {
        "PROJECT_ID": "{project_name_GCP}",
    },
    "CHECKPOINT": {
        "DB_NAME": "{database_name_checkpoint}",
        "DB_SCHEMA": "{checkpoint}",
        "DB_USER": "{user_db_checkpoint}",
        "DB_PWD": "{password_db_checkpoint}",
        "DB_HOST": "{host_ip_db_checkpoint}",
        "DB_PORT": "{port_database_checkpoint}",
    },
    "DB_SOURCE": {
        "DB_NAME": "{database_name_source_data}",
        "DB_USER": "{user_source_data}",
        "DB_PWD": "{password_source_data}",
        "DB_HOST": "{host_ip_source_data}",
        "DB_PORT": "{port_source_data}",
    },
    "DB_DESTINATION": {
        "BUCKET_PATH": "{path}/{to}/{bucket}",
    },
    "MISC": {
        "DRIVER_MSSQL": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }
}


def delete_gcs_path(schema_source, table_source, get_year, get_month, get_date):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('{bucket_name}')
    blobs = bucket.list_blobs(
        prefix=
        f'{path_to_directory_saving_data}/{schema_source}/{table_source}/{get_year}/{get_month}/{get_date}/'
    )

    for blob in blobs:
        blob.delete()
    print(f"Object deleted.")


def retrieve_checkpoint(database_source, schema_source, table_source,
                        checkpoint_type):
    try:
        conn = psycopg2.connect(database=config["CHECKPOINT"]["DB_NAME"],
                                user=config["CHECKPOINT"]["DB_USER"],
                                password=config["CHECKPOINT"]["DB_PWD"],
                                host=config["CHECKPOINT"]["DB_HOST"],
                                port=config["CHECKPOINT"]["DB_PORT"])
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT {checkpoint_type} "
            f"FROM checkpoint.emr_checkpoint "
            f"WHERE schema_name = '{schema_source}' AND table_name = '{table_source}'"
        )
        result = cursor.fetchall()[0]
        conn.commit()
        conn.close()
        last_checkpoint = (result[0])
        return last_checkpoint
    except ConnectionError as connection_error:
        return print("Failed to established connection:", connection_error)


def update_checkpoint(database_source, schema_source, table_source, checkpoint,
                      checkpoint_type):
    try:
        conn = psycopg2.connect(database=config["CHECKPOINT"]["DB_NAME"],
                                user=config["CHECKPOINT"]["DB_USER"],
                                password=config["CHECKPOINT"]["DB_PWD"],
                                host=config["CHECKPOINT"]["DB_HOST"],
                                port=config["CHECKPOINT"]["DB_PORT"])
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(
            f'UPDATE "checkpoint"."emr_checkpoint" '
            f"SET {checkpoint_type} = '{checkpoint}', updated_by = 'Auto' "
            f"WHERE schema_name = '{schema_source}' AND table_name = '{table_source}'"
        )
        conn.commit()
        conn.close()
    except Exception as connection_error:
        print("Failed to established connection:", connection_error)


def write_new_checkpoint(schema_source, table_source, bucket_path,
                         df_checkpoint):
    try:
        sys.stdout = open(f"{table_source}-checkpoint.txt", "w")
        print(df_checkpoint)
        sys.stdout.close()
        os.system(
            f"gsutil mv {table_source}-checkpoint.txt gs://{bucket_path}/{schema_source}/{table_source}/$(date +%Y)/$(date +%m)/$(date +%d)/{table_source}-checkpoint.txt"
        )
    except Exception as exception_message:
        print("Failed to write checkpoint:", exception_message)


def write_none_checkpoint(schema_source, table_source, bucket_path):
    try:
        sys.stdout = open(f"{table_source}-checkpoint.txt", "w")
        print("No Update")
        sys.stdout.close()
        os.system(
            f"gsutil mv {table_source}-checkpoint.txt gs://{bucket_path}/{schema_source}/{table_source}/$(date +%Y)/$(date +%m)/$(date +%d)/{table_source}-checkpoint.txt"
        )
    except Exception as exception_message:
        print("Failed to write checkpoint:", exception_message)


def send_log_success_get(database_source, schema_source, table_source,
                         etl_operation, process_time, df_row_count, cpu_usage,
                         memory_usage, table_type):
    logger.log_struct({
        "process": f"ETL - {database_source} - {table_source}",
        "database": database_source,
        "schema": schema_source,
        "table": table_source,
        "operation": etl_operation,
        "process_time": float(f'{process_time:.2f}'),
        "query_count": df_row_count,
        "cpu_usage": cpu_usage,
        "memory_usage": f'{memory_usage:.2f}',
        "job_type": "etl-job",
        "job_status": "success",
        "job_description": "successfully get new data",
        "status_code": 200,
        "table_type": table_type,
        "log_version": log_version
    })


def send_log_fail_get(database_source, schema_source, table_source,
                      etl_operation, process_time, cpu_usage, memory_usage,
                      table_type):
    logger.log_struct({
        "process": f"ETL - {database_source} - {table_source}",
        "database": database_source,
        "schema": schema_source,
        "table": table_source,
        "operation": etl_operation,
        "process_time": float(f'{process_time:.2f}'),
        "query_count": 0,
        "cpu_usage": cpu_usage,
        "memory_usage": f'{memory_usage:.2f}',
        "job_type": "etl-job",
        "job_status": "failed",
        "job_description": "failed to get new data",
        "status_code": 400,
        "table_type": table_type,
        "log_version": log_version
    })


def send_log_empty_get(database_source, schema_source, table_source,
                       etl_operation, process_time, df_row_count, cpu_usage,
                       memory_usage, table_type):
    logger.log_struct({
        "process": f"ETL - {database_source} - {table_source}",
        "database": database_source,
        "schema": schema_source,
        "table": table_source,
        "operation": etl_operation,
        "process_time": float(f'{process_time:.2f}'),
        "query_count": df_row_count,
        "cpu_usage": cpu_usage,
        "memory_usage": f'{memory_usage:.2f}',
        "job_type": "etl-job",
        "job_status": "success",
        "job_description": "didn't find new data, get process was skipped",
        "status_code": 204,
        "table_type": table_type,
        "log_version": log_version
    })


def send_log_success_load(database_source, schema_source, table_source,
                          etl_operation, process_time, cpu_usage, memory_usage,
                          table_type):
    logger.log_struct({
        "process": f"ETL - {database_source} - {table_source}",
        "database": database_source,
        "schema": schema_source,
        "table": table_source,
        "operation": etl_operation,
        "process_time": float(f'{process_time:.2f}'),
        "cpu_usage": cpu_usage,
        "memory_usage": f'{memory_usage:.2f}',
        "job_type": "etl-job",
        "job_status": "success",
        "job_description": "successfully load new data",
        "status_code": 200,
        "table_type": table_type,
        "log_version": log_version
    })


def send_log_fail_load(database_source, schema_source, table_source,
                       etl_operation, process_time, cpu_usage, memory_usage,
                       table_type):
    logger.log_struct({
        "process": f"ETL - {database_source} - {table_source}",
        "database": database_source,
        "schema": schema_source,
        "table": table_source,
        "operation": etl_operation,
        "process_time": float(f'{process_time:.2f}'),
        "cpu_usage": cpu_usage,
        "memory_usage": f'{memory_usage:.2f}',
        "job_type": "etl-job",
        "job_status": "failed",
        "job_description": "failed to load new data",
        "status_code": 400,
        "table_type": table_type,
        "log_version": log_version
    })


def send_log_empty_load(database_source, schema_source, table_source,
                        etl_operation, process_time, cpu_usage, memory_usage,
                        table_type):
    logger.log_struct({
        "process": f"ETL - {database_source} - {table_source}",
        "database": database_source,
        "schema": schema_source,
        "table": table_source,
        "operation": etl_operation,
        "process_time": float(f'{process_time:.2f}'),
        "cpu_usage": cpu_usage,
        "memory_usage": f'{memory_usage:.2f}',
        "job_type": "etl-job",
        "job_status": "success",
        "job_description": "didn't find new data, load process was skipped",
        "status_code": 204,
        "table_type": table_type,
        "log_version": log_version
    })


def send_log_success_update(database_source, schema_source, table_source,
                            etl_operation, process_time, cpu_usage,
                            memory_usage, table_type):
    logger.log_struct({
        "process": f"ETL - {database_source} - {table_source}",
        "database": database_source,
        "schema": schema_source,
        "table": table_source,
        "operation": etl_operation,
        "process_time": float(f'{process_time:.2f}'),
        "cpu_usage": cpu_usage,
        "memory_usage": f'{memory_usage:.2f}',
        "job_type": "etl-job",
        "job_status": "success",
        "job_description": "successfully update checkpoint",
        "status_code": 200,
        "table_type": table_type,
        "log_version": log_version
    })


def send_log_fail_update(database_source, schema_source, table_source,
                         etl_operation, process_time, cpu_usage, memory_usage,
                         table_type):
    logger.log_struct({
        "process": f"ETL - {database_source} - {table_source}",
        "database": database_source,
        "schema": schema_source,
        "table": table_source,
        "operation": etl_operation,
        "process_time": float(f'{process_time:.2f}'),
        "cpu_usage": cpu_usage,
        "memory_usage": f'{memory_usage:.2f}',
        "job_type": "etl-job",
        "job_status": "failed",
        "job_description": "failed to update checkpoint",
        "status_code": 400,
        "table_type": table_type,
        "log_version": log_version
    })


def send_log_empty_update(database_source, schema_source, table_source,
                          etl_operation, process_time, cpu_usage, memory_usage,
                          table_type):
    logger.log_struct({
        "process": f"ETL - {database_source} - {table_source}",
        "database": database_source,
        "schema": schema_source,
        "table": table_source,
        "operation": etl_operation,
        "process_time": float(f'{process_time:.2f}'),
        "cpu_usage": cpu_usage,
        "memory_usage": f'{memory_usage:.2f}',
        "job_type": "etl-job",
        "job_status": "success",
        "job_description": "didn't find new data, update process was skipped",
        "status_code": 204,
        "table_type": table_type,
        "log_version": log_version
    })
