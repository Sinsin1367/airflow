import itertools
import logging
import os
import re
import stat
import traceback
import time


logger = logging.getLogger(__name__)


def policy(task_instance):
    """
    Airflow policy settings allow for the altering of task instances right
    before they are executed. It allows administrators to rewire some task
    parameters.

    The policy implemented here is a smaller version of the one running in
    production, modified for local use.
    """
    operator_name = task_instance.__class__.__name__
    if not operator_name == 'KubernetesPodOperator':
        return
    dag = task_instance.dag
    dag_id = dag.dag_id
    dag_filepath = dag.fileloc
    product_group_regex = re.compile('.*dags\/([A-z0-9\-+_]+)\/.*')
    try:
        product_group = (product_group_regex
                            .match(dag_filepath)
                            .groups(0)[0]
                            .replace('_', '-'))
    except AttributeError:
        traceback.print_exc()
        msg = ('Product group could not be detected for {dag_id}:{task_id} at '
               'fileloc: {dag_filepath}. Routing to default namespace.')
        logger.error(msg.format(dag_id=dag_id, task_id=task_id,
                                dag_filepath=dag_filepath))
        product_group = 'unknown'
    task_instance.owner = product_group
    task_instance.namespace = 'airflow-tasks'
    task_instance.in_cluster = True
    task_instance.is_delete_operator_pod = False
    task_instance.env_vars.update({'AIRFLOW_PRODUCTGROUP': product_group})
