def spsheet_to_bq(column_name: list, column_datatype: list, bq_project_name,
                  bq_dataset_name, bq_table_name, spsheet_document_id,
                  spsheet_skip_row: int, sheet_name, TRUNCATE_or_APPEND):

    import google.auth
    from google.cloud import bigquery

    # Define credentials, google.auth.default will only work in google environment
    # or in your local if you already logged in using google account with proper credentials
    credentials, project = google.auth.default(scopes=[
        '<https://www.googleapis.com/auth/drive>',
        '<https://www.googleapis.com/auth/bigquery>'
    ])

    # # Specify schema in the destination table, remember excel/spreadsheet is schemaless
    schema = []

    for i, col_name_i in enumerate(column_name):
        print(column_name[i], column_datatype[i])
        schema.append(bigquery.SchemaField(column_name[i], column_datatype[i]))

    # Set project id, dataset name, and table name
    client = bigquery.Client(credentials=credentials, project=project)
    dataset_id = f'{bq_project_name}.{bq_dataset_name}'
    dataset = client.get_dataset(dataset_id)
    table_id = f'{bq_table_name}'
    table = bigquery.Table(dataset.table(table_id), schema=schema)

    external_config = bigquery.ExternalConfig('GOOGLE_SHEETS')
    sheet_url = f'https://docs.google.com/spreadsheets/d/{spsheet_document_id}/edit?usp=sharing'
    external_config.source_uris = [sheet_url]
    # Set how much rows you want to skip, the script will not read that row
    external_config.options.skip_leading_rows = spsheet_skip_row
    # Set sheet and cell range, e.g.: hospital_mapping!A2:C6 if you know the specific cell range,
    # otherwise use only sheet name to get all the data
    external_config.options.range = (f"{sheet_name}")
    table.external_data_configuration = external_config
    external_config.schema = schema

    # Read the data from Google Sheets, turn into Dataframe, and write to BigQuery Table
    job_config = bigquery.QueryJobConfig(
        table_definitions={table_id: external_config})
    query = '''
        SELECT *
        FROM {0}
    '''.format(table_id)
    df = client.query(query, job_config=job_config).to_dataframe()
    # df.project_name = df.project_name.replace({"dwh-siloam": "dwh-siloam_zzz"})
    df = df.dropna(how='all')
    load_job_config = bigquery.LoadJobConfig(
        write_disposition=f'WRITE_{TRUNCATE_or_APPEND}')
    client.load_table_from_dataframe(df,
                                     destination=table,
                                     job_config=load_job_config).result()


column_name = [
    "project_name", "dataset_name", "table_count", "row_count", "table_size"
]
column_datatype = ["STRING", "STRING", "INTEGER", "INTEGER", "FLOAT64"]
bq_project_name = "dwh-siloam"
bq_dataset_name = "TEST"
bq_table_name = "testing_1"
spsheet_document_id = "1MqgEQejWaCbOb2r69erQ0C7vSPYHv5cJIKaloKNjUJc"
spsheet_skip_row = 1
sheet_name = "dataset_metadata"
TRUNCATE_or_APPEND = "TRUNCATE"

spsheet_to_bq(column_name=column_name,
              column_datatype=column_datatype,
              bq_project_name=bq_project_name,
              bq_dataset_name=bq_dataset_name,
              bq_table_name=bq_table_name,
              spsheet_document_id=spsheet_document_id,
              spsheet_skip_row=spsheet_skip_row,
              sheet_name=sheet_name,
              TRUNCATE_or_APPEND=TRUNCATE_or_APPEND)