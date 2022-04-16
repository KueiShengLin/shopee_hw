from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

import sys
sys.path.append("/home/kslin/shopee_hw/airflow/hw_etl")
import hw_etl

# DAG metadata
dag = DAG(
    dag_id="hw_etl",
    start_date=datetime(2022, 4, 16),
    catchup=False,
    tags=["HW"],
    schedule_interval=None,
)

# now = pendulum.now()
now = datetime(2022, 4, 16)

# Dags process
start = DummyOperator(task_id='start_etl', dag=dag)
end = DummyOperator(task_id='end_etl', dag=dag)
basic_data_clean = PythonOperator(task_id="basic_data_clean", op_kwargs={"handle_date": now},
                                  dag=dag, python_callable=hw_etl.basic_data_clean)

start >> basic_data_clean >> end

