from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, decode
from pyspark.sql.types import StructType, StructField, BooleanType, StringType

allowed_domains = ["en.wikipedia.org", "www.wikidata.org", "commons.wikimedia.org"]

spark = SparkSession.builder \
    .appName("FilterBotsRawJson") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

filter_schema = StructType([
    StructField("meta", StructType([
        StructField("domain", StringType())
    ])),
    StructField("performer", StructType([
        StructField("user_is_bot", BooleanType())
    ]))
])

input_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "input") \
    .option("startingOffsets", "latest") \
    .load()

raw_json_df = input_df.select(decode(col("value"), "utf-8").alias("raw_json"))

parsed_df = raw_json_df.select(
    col("raw_json"),
    from_json(col("raw_json"), filter_schema).alias("parsed")
)


filtered_df = parsed_df.filter(
    col("parsed.performer.user_is_bot") == False
).filter(
    col("parsed.meta.domain").isin(allowed_domains)
)
output_df = filtered_df.selectExpr("CAST(raw_json AS STRING) AS value")

query = output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("topic", "processed") \
    .option("checkpointLocation", "/tmp/spark_checkpoint/bot_filter_raw") \
    .start()

query.awaitTermination()
