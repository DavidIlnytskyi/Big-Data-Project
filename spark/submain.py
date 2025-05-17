from pyspark.sql.functions import col, from_json, to_timestamp, hour, lit, decode
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, BooleanType
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaToCassandra") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "uncommonpass") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "processed") \
    .option("startingOffsets", "latest") \
    .load()

json_df = kafka_df.select(decode(col("value"), "utf-8").alias("json_string"))

schema = StructType([
    StructField("meta", StructType([
        StructField("domain", StringType()),
        StructField("uri", StringType())
    ])),
    StructField("dt", StringType()),
    StructField("page_id", LongType()),
    StructField("page_title", StringType()),
    StructField("performer", StructType([
        StructField("user_id", LongType()),
        StructField("user_text", StringType()),
        StructField("user_is_bot", BooleanType())
    ]))
])

parsed_df = json_df.select(from_json(col("json_string"), schema).alias("data"))

flat_df = parsed_df.select(
    col("data.meta.domain").alias("domain"),
    to_timestamp("data.dt").alias("timestamp"),
    col("data.performer.user_id").alias("user_id"),
    col("data.performer.user_text").alias("user_name"),
    col("data.meta.uri").alias("url"),
    col("data.page_id").alias("page_id"),
    col("data.page_title").alias("page_title"),
    col("data.performer.user_is_bot").alias("user_is_bot")
).withColumn("date", col("timestamp").cast("date")) \
 .withColumn("hour", hour("timestamp")) \
 .withColumn("count", lit(1)) \
 .filter(col("user_id").isNotNull())

def write_to_cassandra(df, table):
    df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", f"/tmp/checkpoints/{table}") \
        .option("keyspace", "wiki_stream") \
        .option("table", table) \
        .outputMode("append") \
        .start()

write_to_cassandra(flat_df.select("domain", "hour", "date", "count"), "agg_one")
write_to_cassandra(flat_df.filter(col("user_is_bot") == True)
                   .select("domain", "hour", "date", "count"), "agg_two")
write_to_cassandra(flat_df.select("user_id", "user_name", "timestamp", "date", "hour", "page_title"), "agg_three")
write_to_cassandra(flat_df.select("domain", "count"), "adhoc_one")
write_to_cassandra(flat_df.select("user_id", "timestamp", "url"), "adhoc_two")
write_to_cassandra(flat_df.select("page_id", "timestamp", "url"), "adhoc_four")
write_to_cassandra(flat_df.select("user_id", "user_name", "date", "hour", "timestamp"), "adhoc_five")

spark.streams.awaitAnyTermination()
