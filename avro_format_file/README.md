# Avro Format File

This document was created to help a person who wants to save a data into **Avro** format. The data would be saved to that format by using Apache Spark Framework.

this tutorial will be separated into 3 sections as followed:

1. Checking spark version
2. Create Spark data frame and store it to Avro
3. Read Avro file using Spark

## 1. Checking spark version

In this tutorial, we will use **spark 2.4.8**

Type ‘spark-shell’ on your **command prompt** for checking the spark version.

![Untitled](Avro%20Format%20File/Untitled.png)

## 2. Create Spark data frame and store it to Avro

Before creating file in Avro format, we should create the data frame using Apache Spark. The script below shows how to create the data frame and saving the data into Avro.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession.builder.appName('create_avro') \
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

df.repartition(1).write.format("avro").save("avro_file")
```

When executing the script, we should add some properties when using spark-submit command

```powershell
spark-submit --packages org.apache.spark:spark-avro_2.11:{spark_version} \
	path/to/create_avro.py
```

substitute the spark version and determine the path of our python script

```powershell
spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.8 \
	create_avro.py
```

as the result, the create_avro script format will save the data into avro file 

![Untitled](Avro%20Format%20File/Untitled%201.png)

![Untitled](Avro%20Format%20File/Untitled%202.png)

## 3. Read Avro file using Spark

The provided script will read the Avro format file

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('read_avro').getOrCreate()
data = spark.read.format("avro").load("avro_file")

print(data.show())
print(data.printSchema())
```

and the bash command should use spark-submit and add some properties as follows:

```powershell
spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.8 \
	read_avro.py
```

as the result, the script will give the table information:

![Untitled](Avro%20Format%20File/Untitled%203.png)