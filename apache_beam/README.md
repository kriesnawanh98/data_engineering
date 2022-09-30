# Apache Beam

## 1. What is Apache Beam?
- Open source unified programming model to define both batch and streaming data-processing pipelines
- Beam SDK sre used to create pipeline in the programming language of your choice
- This pipeline can be run in locally on your computer or on backend services with their own features
- RUNNER is used to execute your PIPELINE on a backend of your choice
- Dataflow is of the runners available in BEAM


<img src="image_apache_beam/image.png" width="1600">

Beam provide a comprehensive portability framework for data processing pipelines, that allows you to write your pipeline in specific language with minimal effort.

<img src="image_apache_beam/image1.png" width="1600">
Every runner works with every language

Dataflow runner v2
1.	More efficient and portable worker architecture
2.	Based on apache beam portability framework
3.	Support for multilanguage pipelines and custom containers

<img src="image_apache_beam/image2.png" width="1600">

## 2. Apache Beam using Python script

### 2.1 
```python
import apache_beam as beam
from pysql_beam.sql_io.sql import SQLSource, SQLWriter, ReadFromSQL
from pysql_beam.sql_io.wrapper import MySQLWrapper, PostgresWrapper, SQLDisposition
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions
import pyarrow


class SQLOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--host',
                                           dest='host',
                                           default="localhost")
        parser.add_value_provider_argument('--port',
                                           dest='port',
                                           default="5432")
        parser.add_value_provider_argument('--database',
                                           dest='database',
                                           default="postgres")
        parser.add_value_provider_argument(
            '--query',
            dest='query',
            default="SELECT * FROM public.bimc_table where last_name = 'handy';"
        )
        parser.add_value_provider_argument('--username',
                                           dest='username',
                                           default="postgres")
        parser.add_value_provider_argument('--password',
                                           dest='password',
                                           default="Terminalku_98")


pipeline_options = PipelineOptions()
options = pipeline_options.view_as(SQLOptions)
pipeline = beam.Pipeline(options=options)

mysql_data = (
    pipeline | ReadFromSQL(host=options.host,
                           port=options.port,
                           username=options.username,
                           password=options.password,
                           database=options.database,
                           query=options.query,
                           wrapper=PostgresWrapper)
    | "write" >> beam.io.WriteToParquet(
        'grocery_same',
        pyarrow.schema([('id', pyarrow.int64()),
                        ('first_name', pyarrow.string())])))

pipeline.run().wait_until_finish()

```


### 2.2 
```python
import apache_beam as beam
from pysql_beam.sql_io.sql import SQLSource, SQLWriter, ReadFromSQL
from pysql_beam.sql_io.wrapper import MySQLWrapper, PostgresWrapper, SQLDisposition
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions
import pyarrow


class SQLOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--host',
                                           dest='host',
                                           default="localhost")
        parser.add_value_provider_argument('--port',
                                           dest='port',
                                           default="5432")
        parser.add_value_provider_argument('--database',
                                           dest='database',
                                           default="postgres")
        parser.add_value_provider_argument(
            '--query',
            dest='query',
            default="SELECT * FROM public.bimc_table where last_name = 'handy';"
        )
        parser.add_value_provider_argument('--username',
                                           dest='username',
                                           default="postgres")
        parser.add_value_provider_argument('--password',
                                           dest='password',
                                           default="Terminalku_98")


pipeline_options = PipelineOptions()
options = pipeline_options.view_as(SQLOptions)
pipeline = beam.Pipeline(options=options)

mysql_data = (
    pipeline | ReadFromSQL(host=options.host,
                           port=options.port,
                           username=options.username,
                           password=options.password,
                           database=options.database,
                           query=options.query,
                           wrapper=PostgresWrapper)
            | SQLWriter(host=options.host, port=options.port,
                            username=options.username, password=options.password,
                            database=options.database, table = 'public.bimc_table_out',
                            write_disposition = SQLDisposition.WRITE_TRUNCATE,
                            wrapper=PostgresWrapper
                        )       
                )

pipeline.run().wait_until_finish()

```

### 2.3 
```python
import apache_beam as beam
from pysql_beam.sql_io.sql import SQLSource, SQLWriter, ReadFromSQL
from pysql_beam.sql_io.wrapper import MySQLWrapper, PostgresWrapper
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions
import pyarrow

class SQLOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--host', dest='host', default="localhost")
        parser.add_value_provider_argument('--port', dest='port', default="5432")
        parser.add_value_provider_argument('--database', dest='database', default="postgres")
        parser.add_value_provider_argument('--query', dest='query', default="SELECT id, first_name FROM public.bimc_table;")
        parser.add_value_provider_argument('--username', dest='username', default="postgres")
        parser.add_value_provider_argument('--password', dest='password', default="Terminalku_98")

pipeline_options = PipelineOptions()
options = pipeline_options.view_as(SQLOptions)
pipeline = beam.Pipeline(options=options)

mysql_data = (pipeline | ReadFromSQL(host=options.host, port=options.port,
                                    username=options.username, password=options.password,
                                    database=options.database, query=options.query,
                                    wrapper=PostgresWrapper
                                    )
                        | "write" >> beam.io.WriteToParquet('grocery_same', pyarrow.schema([('id', pyarrow.int64()), ('first_name', pyarrow.string())]))
             )

pipeline.run().wait_until_finish()
```

### 2.4 
```python
import apache_beam as beam
from pysql_beam.sql_io.sql import SQLSource, SQLWriter, ReadFromSQL
from pysql_beam.sql_io.wrapper import MySQLWrapper, PostgresWrapper, SQLDisposition
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions
import pyarrow

class SQLOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--host', dest='host', default="localhost")
        parser.add_value_provider_argument('--port', dest='port', default="5432")
        parser.add_value_provider_argument('--database', dest='database', default="postgres")
        parser.add_value_provider_argument('--query', dest='query', default="SELECT id, first_name FROM public.bimc_table;")
        parser.add_value_provider_argument('--username', dest='username', default="postgres")
        parser.add_value_provider_argument('--password', dest='password', default="Terminalku_98")

pipeline_options = PipelineOptions()
options = pipeline_options.view_as(SQLOptions)
pipeline = beam.Pipeline(options=options)

mysql_data = ( pipeline | "read" >> beam.io.ReadFromParquet('grocery_same.parquet') 
                        | SQLWriter(host=options.host, port=options.port,
                                    username=options.username, password=options.password,
                                    database=options.database, table = 'public.bimc_table_out',
                                    write_disposition = SQLDisposition.WRITE_TRUNCATE,
                                    wrapper=PostgresWrapper
 
             ))

pipeline.run().wait_until_finish()
```