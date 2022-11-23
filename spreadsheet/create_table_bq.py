def run_query_bq(query):
    from google.cloud import bigquery

    client = bigquery.Client()
    query = f"""
                {query}
            """
    query_job = client.query(query, location="asia-southeast2")
    result = query_job.result()

    return result


query = """
CREATE OR REPLACE TABLE TEST.annisa_table(
  project_name STRING,
  dataset_name STRING,
  table_count INT64,
  row_count INT64,
  table_size FLOAT
"""
run_query_bq(query)