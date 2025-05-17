from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, decode, struct
from pyspark.sql.types import StructType, StructField, BooleanType, StringType, LongType
from pyspark.sql.functions import to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
from pyspark.sql.functions import col, from_json

spark = SparkSession.builder \
    .appName("FilterBotsRawJson") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "input") \
    .load()

parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data"))
output_df = parsed_df.select("data.*")

kafka_ready_df = output_df.select(
    lit(None).cast("string").alias("key"),
    to_json(struct(
        col("meta"),
        col("dt"),
        col("page_id"),
        col("page_title"),
        col("performer")
    )).alias("value")
)

query = kafka_ready_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("topic", "processed") \
    .option("checkpointLocation", "/tmp/spark_checkpoint/bot_filter_raw") \
    .start()

query.awaitTermination()
