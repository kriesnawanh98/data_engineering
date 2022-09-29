# Parquet Format File

This document was created to help a person who wants to save a data into **Parquet** format. The data would be saved to that format by using Apache Spark Framework.

this tutorial will be separated into 3 sections as followed:

1. Checking spark version
2. Create Spark data frame and store it to Parquet
3. Read Parquet file using Spark

## 1. Checking spark version

In this tutorial, we will use **spark 2.4.8**

Type ‘spark-shell’ on your **command prompt** for checking the spark version.

![Untitled](Parquet%20Format%20File/Untitled.png)

## 2. Create Spark data frame and store it to Parquet

Before creating file in Parquet format, we should create the data frame using Apache Spark. The script below shows how to create the data frame and saving the data into Parquet.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession.builder.appName('create_parquet') \
    .getOrCreate()

data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show()

df.repartition(1).write.format("parquet").save("parquet_file")
```

Executing script using spark-submit command

```powershell
spark-submit create_parquet.py
```

as the result, the create_parquet script format will save the data into parquet file 

![Untitled](Parquet%20Format%20File/Untitled%201.png)

![Untitled](Parquet%20Format%20File/Untitled%202.png)

![Untitled](Parquet%20Format%20File/Untitled%203.png)

## 3. Read Parquet file using Spark

The provided script will read the Parquet format file

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('read_parquet').getOrCreate()
data = spark.read.format("parquet").load("parquet_file")

data.show()
data.printSchema()
```

and the bash command should use spark-submit as follows:

```powershell
spark-submit read_parquet.py
```

as the result, the script will give the table information:

![Untitled](Parquet%20Format%20File/Untitled%204.png)