# Databricks notebook source
# MAGIC %md
# MAGIC Import libraries

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from  pyspark.sql.functions import col, struct, to_json
from pyspark.sql.types import StructField, StructType, StringType, MapType,DateType,IntegerType,StringType

# COMMAND ----------

# MAGIC %md
# MAGIC Look up table - Date Dimension

# COMMAND ----------

look_up_schema =  StructType([
  StructField('date', StringType(), True),
  StructField('datekey', IntegerType(),True)
])
date_lookup_dict = [
        ('2024-03-16',20240316),
        ('2024-03-17',20240317),
        ('2024-03-18',20240318),
        ('2024-03-19',20240319),
        ('2024-03-20',20240320)
        ]
date_lookup = spark.createDataFrame(data=date_lookup_dict, schema = look_up_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC View of raw data - Loading data from Confluent kafka into a view

# COMMAND ----------

def create_raw_view():
    @dlt.view(comment="Daily kafka daily")
    def raw_view():
      return (
        spark.readStream.format("kafka") 
        .option("kafka.bootstrap.servers", "pkc-w7d6j.germanywestcentral.azure.confluent.cloud:9092") 
        .option("subscribePattern", "topic.*") 
        .option("startingOffsets", "earliest") 
        .option("kafka.security.protocol","SASL_SSL") 
        .option("kafka.sasl.mechanism", "PLAIN") 
        .option("kafka.sasl.jaas.config", """kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="..." 
         password="...";""") 
        .load()
      )

# COMMAND ----------

create_raw_view()

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze Layer with a datekey column - join with a lookup date dimension table

# COMMAND ----------

@dlt.table(
  partition_cols=["topic", "datekey"],
  comment="Bronze layer",
  table_properties={"quality": "bronze"}
)
@dlt.expect("valid_topic", "topic IS NOT NULL")
def daily_raw():
  return (
    dlt.read_stream("raw_view")
      .withColumn("processed_timestamp", F.current_timestamp())
      .join(
        F.broadcast(date_lookup.select("date", "datekey")),
        F.to_date("timestamp") == F.to_date(F.col("date")), "left"
      ).select("value","topic","datekey","timestamp","processed_timestamp")
  )

# COMMAND ----------

# MAGIC %md
# MAGIC Generic function to create Bronze tables - provide parameters accordingly

# COMMAND ----------

def create_bronze_topic_table(table_name, topic, json_schema, select_columns):  
    @dlt.table(
        name=table_name,
        table_properties={"quality": "bronze"},
        comment=f"Parsed streaming data for bronze {topic} records"
    )
    def create_bronze_table():
        return (
            dlt.read_stream("daily_raw")
              .filter(f"topic = '{topic}'")
              .withColumn("body",F.from_json(F.col("value").cast("string"), json_schema))
              .select("body.*","topic","datekey","timestamp","processed_timestamp")
              .select(select_columns)
        )

# COMMAND ----------

# MAGIC %md
# MAGIC Specify table parameters for each table in a dictionary and call the function above to create all the bronze tables respectively

# COMMAND ----------

bronze_tables_config = {
    "bronze_occupation": {
        "topic": "topic_1",
        "json_schema": "id INT, name STRING, occupation STRING, age INT",
        "select_columns": ["id","name", "occupation", "age","topic", "datekey","processed_timestamp","timestamp"]
    },
    "bronze_location": {
        "topic": "topic_2",
        "json_schema": "id INT,middle_name STRING, location STRING,distance INT,update_type STRING",
        "select_columns": ["id","middle_name", "location","distance","update_type","topic","datekey","processed_timestamp","timestamp"]
    }
} 

# COMMAND ----------

# MAGIC %md
# MAGIC # Call function to generate bronze table for each topic

# COMMAND ----------

for target_table, params in bronze_tables_config.items():
    create_bronze_topic_table(target_table, params["topic"], params["json_schema"], params["select_columns"])


# COMMAND ----------

# MAGIC %md
# MAGIC Define rules in a table  - dataframe

# COMMAND ----------

rules_dict = [
        ("topic_1","valid_name","name IS NOT NULL"),
        ("topic_1","valid_occupation","occupation<>'teacher'"),
        ("topic_2","valid_middle_name","middle_name IS NOT NULL"),
        ("topic_2","valid_location","location <>'California'")
        ]
df_rules = spark.createDataFrame(data=rules_dict, schema = ["topic_name","check_name","data_quality_check"])

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Define function to read the data quality rules from the table and create a dictionary

# COMMAND ----------

def get_rules(topic_name):
    df = df_rules.filter(F.col("topic_name") == topic_name)
    rules = {}
    for row in df.collect(): 
        rules[row["check_name"]] = row["data_quality_check"]
    return rules

# COMMAND ----------

# MAGIC %md
# MAGIC Create tables and views with valid and invalid records. 
# MAGIC Quarantine rules are the opposite of the valid data quality rules

# COMMAND ----------

def create_validated_views(dataset, topic, source_table, valid_view, invalid_view):
    rules = get_rules(topic)
    quarantine_rules = f"NOT({' AND '.join(rules.values())})"
    
    @dlt.table(
        name=f"{dataset}",
        partition_cols=["is_quarantined"]
    )
    @dlt.expect_all(rules)
    def create_quarantine():
        return dlt.read_stream(source_table).withColumn("is_quarantined", F.expr(quarantine_rules))
        
    @dlt.view(name=f"{valid_view}")
    def create_valid():
        return dlt.read_stream(f"{dataset}").filter("is_quarantined=false")
    
    @dlt.view(name=f"{invalid_view}")
    def create_invalid():
        return dlt.read_stream(f"{dataset}").filter("is_quarantined=true")    

# COMMAND ----------

quarantine_tables_config = {
    "silver_occupation_quarantine": { 
      "topic_name": "topic_1",
      "source": "bronze_occupation",
      "valid_view": "valid_silver_occupation",
      "invalid_view": "invalid_silver_occupation"
    },
    "silver_location_quarantine": { 
      "topic_name": "topic_2",
      "source": "bronze_location",
      "valid_view": "valid_silver_location",
      "invalid_view": "invalid_silver_location"
    }
} 

for dataset, params in quarantine_tables_config.items():
    create_validated_views(dataset, params["topic_name"], params["source"], params["valid_view"], params["invalid_view"])


# COMMAND ----------

# MAGIC %md
# MAGIC Using salting/hashing and age categorization for extra security we create the silver clean layer only with the valid records. We drop duplicates and apply slowly changing dimension type 1 and 2

# COMMAND ----------

salt = "BEANS"
def salted_hash(id):
    return F.sha2(F.concat(id, F.lit(salt)), 256)   

def age_categorization(age):
    return (
        F.when((age < 18), "under 18")
        .when((age >= 18) & (age < 25), "18-25")
        .when((age >= 25) & (age < 35), "25-35")
        .when((age >= 35) & (age < 45), "35-45")
        .otherwise("REDACTED")
        .alias("age")
    )

@dlt.table(table_properties={"quality":"silver"})
def silver_occupation_clean():
    return (
        dlt.read_stream("valid_silver_occupation")
            .withColumn("alt_id",salted_hash(F.col("id")))
            .withColumn("updated",F.col("processed_timestamp").cast("timestamp"))
            .withColumn("age_brackets", age_categorization(F.col("age")))
            .withWatermark("updated", "30 seconds")
            .dropDuplicates(["alt_id", "updated"])
    )


@dlt.table(table_properties={"quality": "silver"})
def silver_location_clean():
    return (
        dlt.read_stream("valid_silver_location")
          .withColumn("updated",F.col("processed_timestamp").cast("timestamp"))
          .withWatermark("updated", "30 seconds")
          .dropDuplicates(["id","update_type","updated"])
          .withColumn("distance_check", F.when(F.col("distance") <= 10, "Invalid distance").otherwise("OK"))
          .withColumn("alt_id",salted_hash(F.col("id")))
    )


dlt.create_streaming_live_table(
    name="location_clean", 
    table_properties={"quality": "silver"}
)
dlt.apply_changes(
    target = "location_clean",
    source = "silver_location_clean",
    keys = ["alt_id"],
    sequence_by = F.col("timestamp"),
    apply_as_deletes = F.expr("update_type = 'delete'"),
    except_column_list = ["is_quarantined"]
)

# SCD Type 2
dlt.create_streaming_live_table(
  name="location_clean_SCD2", 
  table_properties={"quality": "silver"}
)
dlt.apply_changes(
  target = "location_clean_SCD2", 
  source = "silver_location_clean",
  keys = ["alt_id"],
  sequence_by = F.col("timestamp"),
  apply_as_deletes = F.expr("update_type = 'delete'"),
  except_column_list = ["is_quarantined"],
  stored_as_scd_type = "2" 
)


# COMMAND ----------

# MAGIC %md
# MAGIC Creating Gold layer by combining the two datasets and creating an aggragated view for business users

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------


@dlt.table(comment="Gold layer")
def gold_aggregated_layer():
     return spark.sql(f"""
      SELECT  a.datekey,a.id,b.name,avg(a.distance) as average_distance
      FROM (
        SELECT id,middle_name,location,distance,datekey
        FROM LIVE.location_clean_SCD2
        WHERE distance_check = "OK" AND __END_AT IS NULL) a
      LEFT JOIN (
        SELECT id,name, occupation, age
        FROM LIVE.silver_occupation_clean
        WHERE age_brackets <> "REDACTED") b
      ON a.id = b.id
      GROUP BY a.datekey,a.id,b.name
    """)
