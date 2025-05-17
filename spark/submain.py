from pyspark.sql.functions import col, from_json, to_timestamp, hour, expr, lit, decode
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, BooleanType, TimestampType, MapType, StructType
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

performer_schema = StructType([
    StructField("user_text", StringType()),
    StructField("user_groups",
                StringType(), True),
    StructField("user_is_bot", BooleanType()),
    StructField("user_id", LongType()),
    StructField("user_registration_dt", StringType()),
    StructField("user_edit_count", LongType())
])

rev_slots_schema = StructType([
    StructField("main", StructType([
        StructField("rev_slot_content_model", StringType()),
        StructField("rev_slot_sha1", StringType()),
        StructField("rev_slot_size", LongType()),
        StructField("rev_slot_origin_rev_id", LongType())
    ]))
])

meta_schema = StructType([
    StructField("uri", StringType()),
    StructField("request_id", StringType()),
    StructField("id", StringType()),
    StructField("dt", StringType()),
    StructField("domain", StringType()),
    StructField("stream", StringType()),
    StructField("topic", StringType()),
    StructField("partition", IntegerType()),
    StructField("offset", LongType())
])

schema = StructType([
    StructField("$schema", StringType()),
    StructField("meta", meta_schema),
    StructField("database", StringType()),
    StructField("page_id", LongType()),
    StructField("page_title", StringType()),
    StructField("page_namespace", IntegerType()),
    StructField("rev_id", LongType()),
    StructField("rev_timestamp", StringType()),
    StructField("rev_sha1", StringType()),
    StructField("rev_minor_edit", BooleanType()),
    StructField("rev_len", LongType()),
    StructField("rev_content_model", StringType()),
    StructField("rev_content_format", StringType()),
    StructField("performer", performer_schema),
    StructField("page_is_redirect", BooleanType()),
    StructField("comment", StringType()),
    StructField("parsedcomment", StringType()),
    StructField("dt", StringType()),
    StructField("rev_slots", rev_slots_schema)
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
 .withColumn("count", lit(1))


def write_to_cassandra(df, table):
    df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", f"/tmp/checkpoints/{table}") \
        .option("keyspace", "wiki_stream") \
        .option("table", table) \
        .outputMode("append") \
        .start()

agg_df = flat_df.select("domain", "hour", "date", "timestamp")
write_to_cassandra(agg_df, "agg_one")

agg_two_df = flat_df.filter(col("user_is_bot") == True) \
    .select("domain", "hour", "date", "timestamp")
write_to_cassandra(agg_two_df, "agg_two")

agg_three_df = flat_df.select("user_id", "user_name", "timestamp", "date", "hour", "page_title")
write_to_cassandra(agg_three_df, "agg_three")

adhoc_one_df = flat_df.select("domain", "count")
write_to_cassandra(adhoc_one_df, "adhoc_one")

adhoc_two_df = flat_df.select("user_id", "timestamp", "url")
write_to_cassandra(adhoc_two_df, "adhoc_two")

adhoc_three_df = flat_df.select("domain", "timestamp", "url")
write_to_cassandra(adhoc_three_df, "adhoc_three")

adhoc_four_df = flat_df.select("page_id", "timestamp", "url")
write_to_cassandra(adhoc_four_df, "adhoc_four")

adhoc_five_df = flat_df.select("user_id", "user_name", "timestamp", "hour")
write_to_cassandra(adhoc_five_df, "adhoc_five")

spark.streams.awaitAnyTermination()
