from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExportToPostgres") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta").load("/opt/airflow/data/delta/fact_sales")

df.write \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://postgres:5432/airflow") \
  .option("dbtable", "delta_sales") \
  .option("user", "airflow") \
  .option("password", "airflow") \
  .option("driver", "org.postgresql.Driver") \
  .mode("overwrite") \
  .save()
