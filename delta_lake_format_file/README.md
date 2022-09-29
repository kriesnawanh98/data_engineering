# Delta Lake Format File

This document was created to help a person who wants to save a data into **Delta Lake** format. The data would be saved to that format by using Apache Spark Framework.

this tutorial will be separated into 3 section as followed:

1. Checking spark version
2. Install delta-spark module
3. Create Spark data frame and store it to Delta Lake
4. Read Delta Lake file using Spark

## 1. Checking spark version

In this tutorial, we will use **spark 3.2.2**. 

Type ‘spark-shell’ on your **command prompt** for checking the spark version.

![Untitled](Delta%20Lake%20Format%20File/Untitled.png)

## 2. Install delta-spark module

Before installing the delta-spark module, we should check the compatibility with the spark version.

the compatibility can be seen on : 

[](https://docs.delta.io/latest/releases.html)

![Untitled](Delta%20Lake%20Format%20File/Untitled%201.png)

We will install the Delta Lake module with version of **2.0.0** because the Spark version is **3.2.2** in this case.

we can easily install it using pip command, as followed:

![Untitled](Delta%20Lake%20Format%20File/Untitled%202.png)

## 3. Create Spark data frame and store it to Delta Lake

Before creating file in Delta Lake format, we should create the data frame using Apache Spark. The script below shows how to create the data frame and also import the delta module in the beginning of the script for saving the data into delta lake. In the end, the script would create the Delta Lake format file.

```python
import pyspark
from delta import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

builder = pyspark.sql.SparkSession.builder.appName("create_deltalake")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

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

df.repartition(1).write.format("delta").save("delta-table") # create file in delta lake format
```

When executing the script, we should add some properties when using spark-submit command

```powershell
spark-submit --packages io.delta:delta-core_2.12:{delta_version} \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
	path/to/create_delta_lake_file.py
```

substitute the delta version and determine the path of our python script

```powershell
spark-submit --packages io.delta:delta-core_2.12:2.0.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
	create_deltalake.py
```

as the result, the delta script format will save the data into parquet 

![Untitled](Delta%20Lake%20Format%20File/Untitled%203.png)

![Untitled](Delta%20Lake%20Format%20File/Untitled%204.png)

## 4. Read Delta Lake file using Spark

The provided script will read the delta lake format file

```python
import pyspark
from delta import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

builder = pyspark.sql.SparkSession.builder.appName("read_deltalake")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.format("delta").load("delta-table")
df.printSchema()
df.show()
```

and the bash command should use spark-submit and add some properties as follows:

```powershell
spark-submit --packages io.delta:delta-core_2.12:2.0.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  read_deltalake.py
```

as the result, the script will give the table information:

![Untitled](Delta%20Lake%20Format%20File/Untitled%205.png)