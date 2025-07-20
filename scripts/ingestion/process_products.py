from common import get_spark_session

spark = get_spark_session("ProcessProducts")
df = spark.read.csv("/opt/airflow/data/products/products.csv", header=True, inferSchema=True)
df.write.format("parquet").mode("overwrite").save("/opt/airflow/data/delta/dim_product")
