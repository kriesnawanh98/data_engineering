# APACHE Airflow
Apache Airflow is an open-source platform for developing, scheduling, and monitoring batch-oriented workflows. Airflow’s extensible ```Python framework``` enables you to build workflows connecting with virtually any technology. A web interface helps manage the state of your workflows. Airflow is deployable in many ways, varying from a single process on your laptop to a distributed setup to support even the biggest workflows

## Workflows as code
The main characteristic of Airflow workflows is that all workflows are defined in Python code. “Workflows as code” serves several purposes:

- ```Dynamic```: Airflow pipelines are configured as Python code, allowing for dynamic pipeline generation.

- ```Extensible```: The Airflow framework contains operators to connect with numerous technologies. All Airflow components are extensible to easily adjust to your environment.

- ```Flexible```: Workflow parameterization is built-in leveraging the Jinja templating engine

## 1. Example of DAG script in Airflow
```python
from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'DAG_owner',
    'start_date': datetime(2022, 8, 28),
    'email': [
        'email@email.com'
    ],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'DAG_name',
        description='Scheduler - description',
        schedule_interval='0 3 * * 1', #cron expression
        default_args=default_args,
        tags=['example_dag'],
        catchup=False) as dag_exp:
    t1 = BashOperator(
        task_id='task_1',
        bash_command=
        'cd ~/path_to_job/ && python3 job_1.py',
        dag=dag_exp)

    t2 = BashOperator(
        task_id='task_2',
        bash_command=
        'cd ~/path_to_job/ && python3 job_2.py',
        dag=dag_exp)

    t3 = BashOperator(
        task_id='task_3',
        bash_command=
        'cd ~/path_to_job/ && python3 job_3.py',
        dag=dag_exp)

    t1 >> t2 >> t3
```