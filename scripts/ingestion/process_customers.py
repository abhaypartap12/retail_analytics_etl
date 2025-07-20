from common import get_spark_session

spark = get_spark_session("ProcessCustomers")
df = spark.read.csv("/opt/airflow/data/customers/customers.csv", header=True, inferSchema=True)
df.write.format("parquet").mode("overwrite").save("/opt/airflow/data/delta/dim_customer")
