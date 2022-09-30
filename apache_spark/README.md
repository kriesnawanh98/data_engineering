# APACHE Spark

## 1. Starting Point: SparkSession
The entry point into all function in Spark is **SparkSession**
For creating the SparkSession:
### 1.1 Version 1
```python
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

```
### 1.2 Version 2
```python
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf()
conf.setAppName("Sync-asterisk1-Get")
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)
```

## 2. Read JSON files
```python
peopleDF = spark.read.json("{path}/file.json")
```

## 3. Write JSON files
```python
peopleDF.write.json("{path}/file.json")
```

## 4. Read PARQUET files
```python
peopleDF = spark.read.parquet("{path}/file.parquet")
```

## 5. write PARQUET files
```python
peopleDF.write.parquet("{path}/file.parquet")
```

## 6. Read CSV files
```python
peopleDF = spark.read.csv("{path}/file.csv")
```

## 7. write CSV files
```python
peopleDF.write.csv("{path}/file.csv")
```

## 8. JDBC to RDMBS database
### 8.1 Read and Write data in MySQL 
```python
sql = f"(SELECT * FROM {[TABLE_MYSQL]}) t1"
driver_mysql = "com.mysql.jdbc.Driver"

df = spark.read.format("jdbc") \
    .option("url", f"jdbc:mysql://{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_SOURCE}") \
    .option("dbtable", sql) \
    .option("user", [USER_SOURCE]) \
    .option("password", [PASSWORD_SOURCE]) \
    .option("driver", driver_mysql) \
    .load()

df.write \
    .format("jdbc") \
    .option("url", f"jdbc:mysql://{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_SOURCE}") \
    .option("dbtable", [TABLE_MYSQL]) \
    .option("user", [USER_SOURCE]) \
    .option("password", [PASSWORD_SOURCE]) \
    .option("driver", driver_mysql) \
    .save()
```
### 8.2 Read and Write data in PostgreSQL
```python
sql =   f"(SELECT * FROM {[TABLE_SCHEMA]}.{[TABLE_POSTGRESQL]}) t1"
driver_postgresql = "com.postgresql.jdbc.Driver"

df = spark.read.format("jdbc") \
    .option("url", f"jdbc:postgresql://{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_SOURCE}") \
    .option("dbtable", sql) \
    .option("user", [USER_SOURCE]) \
    .option("password", [PASSWORD_SOURCE]) \
    .option("driver", driver_postgresql) \
    .load()

df.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_SOURCE}") \
    .option("dbtable", [TABLE_SCHEMA].[TABLE_POSTGRESQL]) \
    .option("user", [USER_SOURCE]) \
    .option("password", [PASSWORD_SOURCE]) \
    .option("driver", driver_postgresql) \
    .save()
```
### 8.3 Read and Write data in SQLServer
```python
sql = f"SELECT * FROM {[TABLE_SCHEMA]}.{[TABLE_MSSQL]}"
driver_mssql = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

df = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://{[DATABASE_HOST]};database={[DATABASE_SOURCE]}") \
    .option("dbtable", sql) \
    .option("user", [USER_SOURCE]) \
    .option("password", [PASSWORD_SOURCE]) \
    .option("driver", driver_mssql) \
    .load()

df.write \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://{[DATABASE_HOST]};database={[DATABASE_SOURCE]}") \
    .option("dbtable", [TABLE_SCHEMA].[TABLE_MSSQL]) \
    .option("user", [USER_SOURCE]) \
    .option("password", [PASSWORD_SOURCE]) \
    .option("driver", driver_mssql) \
    .save()
```
