# Google Cloud Platform (GCP) commad
We should run the command on **Terminal** in **GCP**

## 1. How to run spark submit in dataproc-cluster
```
gcloud dataproc jobs submit pyspark gs://[path_to_file]/file.py \
--cluster=[dataproc_cluster name/id] --region=[region ex: asia-southeast2] \
--jars=gs://[path_to_RDBMS_jar_file]/[jarfile ex:postgresql-42.2.12].jar \
--properties=spark.driver.extraClassPath=[jarfile ex:postgresql-42.2.12].jar \
--py-files=gs://[path_to_file_additional]/[additional_file_name].py

```

##