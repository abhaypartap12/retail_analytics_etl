from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat_ws, sha2
from pyspark.sql.types import StructType, IntegerType, StringType, TimestampType
from delta.tables import DeltaTable
import os

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("KafkaToDeltaUpsertJob") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()

# Define schema for the Kafka message
schema = StructType() \
    .add("sale_id", StringType()) \
    .add("customer_id", IntegerType()) \
    .add("product_id", IntegerType()) \
    .add("quantity", IntegerType()) \
    .add("sale_timestamp", TimestampType())

# Read batch from Kafka
raw_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9093") \
    .option("subscribe", "transaction-events") \
    .load()

# Extract and parse JSON, build base dataframe
sales_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Join with dimension data if needed (optional)
dim_customer = spark.read.parquet("/opt/airflow/data/delta/dim_customer")
dim_product = spark.read.parquet("/opt/airflow/data/delta/dim_product")

fact_sales = sales_df \
    .join(dim_customer, "customer_id", "left") \
    .join(dim_product, "product_id", "left")

# Create a hash column for identifying unique sales records
fact_sales_hashed = fact_sales.withColumn(
    "sale_hash",
    sha2(concat_ws("||", col("sale_id"), col("customer_id"), col("product_id")), 256)
)

# Delta target table path
delta_path = "file:///opt/airflow/data/delta/fact_sales"

# Check if Delta table exists
if DeltaTable.isDeltaTable(spark, delta_path):
    delta_table = DeltaTable.forPath(spark, delta_path)
    # Upsert (merge) logic
    delta_table.alias("target").merge(
        fact_sales_hashed.alias("source"),
        "target.sale_hash = source.sale_hash"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print("✅ Upsert complete into existing Delta table.")
else:
    # First-time write
    fact_sales_hashed.write.format("delta").mode("overwrite").save(delta_path)
    print("✅ Delta table created with initial data.")

spark.stop()
