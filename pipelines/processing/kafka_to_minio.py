from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType

# Definir schema dos produtos
product_schema = StructType() \
    .add("id", IntegerType()) \
    .add("title", StringType()) \
    .add("price", FloatType()) \
    .add("description", StringType()) \
    .add("category", StringType()) \
    .add("image", StringType()) \
    .add("rating", StructType()
         .add("rate", FloatType())
         .add("count", IntegerType()))

# Inicializar Spark
spark = SparkSession.builder \
    .appName("KafkaToMinIO") \
    .getOrCreate()

# Lê mensagens do Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "products") \
    .option("startingOffsets", "latest") \
    .load()

# Extrai JSON
df_json = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), product_schema)) \
    .select("data.*")

# Salva no MinIO (via S3 API)
df_json.writeStream \
    .format("json") \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .option("path", "s3a://data-lake/raw_v1/products/") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
