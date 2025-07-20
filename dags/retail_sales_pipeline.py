from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 2),
    'retries': 1,
}

with DAG(
    dag_id='retail_sales_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['retail', 'spark', 'kafka', 'superset'],
) as dag:

    start = DummyOperator(task_id='start')

    process_customers = BashOperator(
        task_id='process_customers',
        bash_command="""
        docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
            /opt/airflow/scripts/ingestion/process_customers.py
        """
    )

    process_products = BashOperator(
        task_id='process_products',
        bash_command="""
        docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
            /opt/airflow/scripts/ingestion/process_products.py
        """
    )

    simulate_kafka_events = BashOperator(
        task_id='simulate_kafka_events',
        bash_command="""
        python3 /opt/airflow/scripts/ingestion/kafka_producer.py
        """
    )

    run_spark_job = BashOperator(
        task_id='run_spark_job',
        bash_command="""
        docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
            --conf spark.pyspark.python=/opt/bitnami/python/bin/python \
            --conf spark.pyspark.driver.python=/opt/bitnami/python/bin/python \
            --master spark://spark-master:7077 \
            --jars /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar,\
/opt/bitnami/spark/jars/kafka-clients-3.5.1.jar,\
/opt/bitnami/spark/jars/kafka_2.12-3.5.1.jar,\
/opt/bitnami/spark/jars/commons-pool2-2.11.1.jar,\
/opt/bitnami/spark/jars/delta-spark_2.12-3.0.0.jar,\
/opt/bitnami/spark/jars/delta-storage-3.0.0.jar,\
/opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar \
            /opt/airflow/scripts/transformation/process_sales.py
        """
    )

    export_to_postgres = BashOperator(
        task_id='export_to_postgres',
        bash_command="""
        docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
            --conf spark.pyspark.python=/opt/bitnami/python/bin/python \
            --conf spark.pyspark.driver.python=/opt/bitnami/python/bin/python \
            --master spark://spark-master:7077 \
            --jars /opt/bitnami/spark/jars/postgresql-42.7.1.jar,\
/opt/bitnami/spark/jars/delta-spark_2.12-3.0.0.jar,\
/opt/bitnami/spark/jars/delta-storage-3.0.0.jar \
            /opt/airflow/scripts/load/load_to_postgres.py
        """
    )

    bootstrap_superset = BashOperator(
        task_id='bootstrap_superset',
        bash_command="""
        docker exec superset python /app/dashboards/init_superset.py
        """
    )

    end = DummyOperator(task_id='end')

    start >> [process_customers, process_products] >> simulate_kafka_events >> run_spark_job >> export_to_postgres >> bootstrap_superset >> end
