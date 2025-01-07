# dags/spark_dags.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_dags',
    default_args=default_args,
    description='A simple DAG to run Spark scripts',
    schedule_interval=timedelta(days=1),
    concurrency=4,  # Tăng số lượng task có thể chạy đồng thời
)

graph_processing_task = BashOperator(
    task_id='graph_processing',
    bash_command='python /opt/airflow/python-scripts/spark/graph_processing.py',
    dag=dag,
)

machine_learning_task = BashOperator(
    task_id='machine_learning',
    bash_command='python /opt/airflow/python-scripts/spark/machine_learning.py',
    dag=dag,
)

rf_task = BashOperator(
    task_id='random_forest',
    bash_command='python /opt/airflow/python-scripts/spark/rf.py',
    dag=dag,
)

statistics_task = BashOperator(
    task_id='statistics',
    bash_command='python /opt/airflow/python-scripts/spark/statistics.py',
    dag=dag,
)

# Các task chạy đồng thời
[graph_processing_task, machine_learning_task, rf_task, statistics_task]


producer_consumer_dag = DAG(
    'producer_consumer_dag',
    default_args=default_args,
    description='A DAG to run producer and consumer scripts every 30 minutes',
    schedule_interval=timedelta(minutes=30),
    concurrency=4,
)

# Task chạy nhiều lần
producer_task = BashOperator(
    task_id='producer',
    bash_command='python /opt/airflow/python-scripts/spark_stream/producer.py',
    dag=producer_consumer_dag,
)

consumer_task = BashOperator(
    task_id='consumer',
    bash_command='python /opt/airflow/python-scripts/spark_stream/consumer.py',
    dag=producer_consumer_dag,
)

# producer chạy trước consumer
producer_task >> consumer_task