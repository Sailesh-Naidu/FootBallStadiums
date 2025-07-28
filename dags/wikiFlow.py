from airflow import DAG
from datetime import datetime
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow.operators.python import PythonOperator

from pipelines.getWikiData import extract_wikipedia_data, transform_wikipedia_data, write_wikipedia_data

dag = DAG(
    dag_id = 'wikipedia_flow',
    default_args = {
        "owner" : 'Sailesh',
        "start_date" : datetime(2025, 7, 18)
    },
    schedule_interval= None,
    catchup=False
)

extract_data_from_wiki = PythonOperator(
    task_id= "extract_data_from_wiki",
    python_callable= extract_wikipedia_data,
    op_kwargs = {"url" : "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
    provide_context= True,
    dag = dag
)

transform_fetched_data = PythonOperator(
    task_id = 'transform_fetched_data',
    python_callable = transform_wikipedia_data,
    provide_context= True,
    dag = dag
)

write_wikipedia_data = PythonOperator(
    task_id= "write_wikipedia_data",
    provide_context= True,
    python_callable= write_wikipedia_data,
    dag = dag
)

extract_data_from_wiki >> transform_fetched_data >> write_wikipedia_data
