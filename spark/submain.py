from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, decode, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType

full_schema = StructType([
    StructField("meta", StructType([
        StructField("domain", StringType()),
        StructField("dt", StringType())
    ])),
    StructField("performer", StructType([
        StructField("user_id", IntegerType())
    ])),
    StructField("page_title", StringType())
])

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

parsed_df = json_df.select(from_json(col("json_string"), full_schema).alias("data"))

result_df = parsed_df.select(
    col("data.performer.user_id").alias("user_id"),
    col("data.meta.domain").alias("domain"),
    to_timestamp(col("data.meta.dt")).alias("created_at"),
    col("data.page_title").alias("page_title")
)

query = result_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "wiki_data") \
    .option("table", "results") \
    .option("checkpointLocation", "/tmp/spark_checkpoint/kafka_to_cassandra") \
    .outputMode("append") \
    .start()

query.awaitTermination()
