def run_query_bq_to_list(columns_list: list, table_url):
    from google.cloud import bigquery
    columns_query = ', '.join(columns_list)

    client = bigquery.Client()
    query = f"""
            SELECT
            {columns_query} 
            FROM 
            `{table_url}`
        """
    query_job = client.query(query, location="asia-southeast2")
    result = query_job.result()

    df_result = result.to_dataframe()

    list_results = df_result.values.tolist()

    return list_results


def list_to_spreadsheet(spreadsheet_id, sheet_name, start_cell, list_results):
    import google.auth
    from googleapiclient import discovery

    credentials, project = google.auth.default(scopes=[
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/bigquery'
    ])

    # Writing to Spreadsheet using Spreadsheet API v4
    service = discovery.build('sheets', 'v4', credentials=credentials)
    spreadsheet_id = spreadsheet_id

    # Specify cell range in google sheets
    range_ = f'{sheet_name}!{start_cell}'
    value_input_option = 'RAW'
    value_range_body = {"values": list_results}

    request = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_,
        valueInputOption=value_input_option,
        body=value_range_body)

    response = request.execute()
    return response


column_list_dataset = [
    "project_name", "dataset_name", "table_count", "row_count", "table_size"
]
table_url_dataset = """dwh-siloam.TABLE_INFO.DATASET_INFO"""
spreadsheet_id = '1MqgEQejWaCbOb2r69erQ0C7vSPYHv5cJIKaloKNjUJc'
sheet_name_dataset = 'dataset_metadata'
start_cell_dataset = 'A2'

list_results_dataset = run_query_bq_to_list(column_list_dataset,
                                            table_url_dataset)
response_dataset = list_to_spreadsheet(spreadsheet_id=spreadsheet_id,
                                       sheet_name=sheet_name_dataset,
                                       start_cell=start_cell_dataset,
                                       list_results=list_results_dataset)
