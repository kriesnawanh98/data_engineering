import google.auth
from google.cloud import bigquery
import psycopg2

credentials, project = google.auth.default(scopes=[
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/bigquery'
])

schema = [
    bigquery.SchemaField('maintenance_index', 'INTEGER'),
    bigquery.SchemaField('activity', 'STRING'),
    # bigquery.SchemaField('activity_type', 'STRING'),
    # bigquery.SchemaField('date_start', 'DATE'),
    # bigquery.SchemaField('man_days', 'INTEGER'),
    # bigquery.SchemaField('problem', 'STRING'),
    # bigquery.SchemaField('solution', 'STRING'),
    # bigquery.SchemaField('maintenance_status', 'STRING'),
    # bigquery.SchemaField('pic', 'STRING'),
    # bigquery.SchemaField('notes', 'STRING'),
]

client = bigquery.Client(credentials=credentials, project=project)
dataset_id = '[project].[dataset]'
dataset = client.get_dataset(dataset_id)
table_id = '[table_name]'
table = bigquery.Table(dataset.table(table_id), schema=schema)

external_config = bigquery.ExternalConfig('GOOGLE_SHEETS')
sheet_url = 'https://docs.google.com/spreadsheets/d/1212fsajifjai121ijijijdisf/edit?usp=sharing'
external_config.source_uris = [sheet_url]
external_config.options.skip_leading_rows = 3
# external_config.options.range = ("Sheet1")
table.external_data_configuration = external_config
external_config.schema = schema

job_config = bigquery.QueryJobConfig(
    table_definitions={table_id: external_config})
query = '''
    SELECT *
    FROM {0}
'''.format(table_id)
df_ori = client.query(query, job_config=job_config).to_dataframe()

df = df_ori.dropna(how='all')
# df.head()
# df.info()

df_list = list(df.itertuples(index=False))
# df_list[1]

conn = psycopg2.connect(host="[host_ip]",
                        port="[port]",
                        user="[user]",
                        password="[password]",
                        database="[database]")

cursor = conn.cursor()

for i, data in enumerate(df_list):
    cursor.execute(
        """
            INSERT INTO [schema].[table_name](
                col_1,
                col_2            
            ) 
            VALUES(
                %s,
                %s
            )
        """, data)
    print(i)

conn.commit()
cursor.close()
conn.close()