# Databricks notebook source
import pyspark.sql.functions as F
from  pyspark.sql.functions import col, struct, to_json
from pyspark.sql.types import StructField, StructType, StringType, MapType

# COMMAND ----------

dataDictionary = [
        (1,'James','driver',15),
        (2,'Michael','teacher',18),
        (3,'Robert','engineer',23),
        (4,'Washington','architect',30),
        (5,'Jefferson','CEO',67)
        ]
df = spark.createDataFrame(data=dataDictionary, schema = ["id","name","occupation","age"])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

(df.selectExpr("name AS key", "to_json(struct(*)) AS value") \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "pkc-w7d6j.germanywestcentral.azure.confluent.cloud:9092") \
  .option("topic", "topic_1") \
  .option("kafka.security.protocol","SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.sasl.jaas.config", """kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="..." password="...";""") \
  .save()
)

# COMMAND ----------

dataDictionary2 = [
        (1,'Jonas','NYC',25,'insert'),
        (2,'Taylor','California',20,'insert'),
        (3,'Perry','Louisiana',55,'insert'),
        (4,'Denzel','Texas',34,'insert'),
        (5,'Kate','Michigan',7,'insert')
        ]
df2 = spark.createDataFrame(data=dataDictionary2, schema = ["id","middle_name","location","distance","update_type"])
df2.printSchema()
df2.show(truncate=False)

# COMMAND ----------

(df2.selectExpr("middle_name AS key", "to_json(struct(*)) AS value") \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "pkc-w7d6j.germanywestcentral.azure.confluent.cloud:9092") \
  .option("topic", "topic_2") \
  .option("kafka.security.protocol","SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.sasl.jaas.config", """kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="..." password="...";""") \
  .save()
)

# COMMAND ----------

dataDictionary2 = [
        (5,'Kate','Oregon',7,'update'),
        (4,'Denzel','Texas',34,'delete')
        ]
df2 = spark.createDataFrame(data=dataDictionary2, schema = ["id","middle_name","location","distance","update_type"])
df2.printSchema()
df2.show(truncate=False)

# COMMAND ----------

(df2.selectExpr("middle_name AS key", "to_json(struct(*)) AS value") \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "pkc-w7d6j.germanywestcentral.azure.confluent.cloud:9092") \
  .option("topic", "topic_2") \
  .option("kafka.security.protocol","SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.sasl.jaas.config", """kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="..." password="....";""") \
  .save()
)
