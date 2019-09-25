from __future__ import (absolute_import, division, print_function, unicode_literals)
from builtins import *

import logging as log
import datetime

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

default_args = {
                'depends_on_past': False,
                'email': ['sinaa@indeed.com'],
                'email_on_failure': True,
                'email_on_retry': False,
                'start_date': datetime.datetime(2019, 4, 9),
                'retries': 0,
                'retry_delay': datetime.timedelta(minutes=5)
                }




def _get_task_opertator(task_id,dag):
    log.info('Creating task with task_id: "{}".'.format(task_id))
    op = KubernetesPodOperator(
        task_id=task_id,
        name=task_id,
        owner= 'sinaa',
        email= ['sinaa@indeed.com'], #Todo: get table owner email and set it here
        namespace='auto-join',
        in_cluster=True,
        is_delete_operator_pod=True,
        startup_timeout_seconds=420,  # Larger value for larger image.
        image='registry.corp.indeed.com/squall/airflow-operators/autojoin/sina-test-container:latest',
        image_pull_policy='Always',
        env_vars={
            'AIRFLOW_PRODUCTGROUP': 'auto-join'
        },
        secrets=[
        ],
        arguments=[],
        image_pull_secrets='registry',
        dag=dag,
    )
    return op

# Driver
dag_id = 'sina-test-dag'
schedule = datetime.timedelta(days=1)
log.info('Creating dag: "{}".'.format(dag_id))
dag = DAG(
    dag_id=dag_id,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    is_paused_upon_creation=False
)

with dag:
    test_task = _get_task_opertator('sina-test-task', dag)
