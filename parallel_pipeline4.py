import json
import os
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
import pendulum
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator


def get_conf(file_path):
    config = json.loads(open(file_path, 'r').read())
    return config


CUR_DIR = os.path.abspath(os.path.dirname(__file__))
conf = get_conf(f'{CUR_DIR}/conf.json')


with DAG(
    'parallel_pipeline',
    default_args={
        'depends_on_past': False,
    },
    description='Parallel Pipeline',
    schedule_interval=None,
    start_date=datetime(2022, 4, 20),
    catchup=False,
    tags=['pipeline'],
) as dag:
    start_task = DummyOperator(
        task_id='start_parallel_task'
    )
    parallel_tasks = []
    for host_ip in conf.get('hosts', []):
        hook = SSHHook(
            remote_host=host_ip,
            username=conf.get('remote_username', 'gpudb'),
            password=conf.get('remote_password', 'gisfed11')
        )
        parallel_tasks.append(
            SSHOperator(
                task_id=f"Runs_On_Remote_Host_{host_ip}",
                remote_host=host_ip,
                ssh_hook=hook,
                command=f"pushd /home/gpudb/golui/gpudb-qa/kinetica-python-recordretriever-test; ./bin/test-regression.sh --rat -u 'admin' -w 'Kinetica1!' --tuser 'admin' --tpwd 'Kinetica1!'; popd"
                # command=f"pushd /home/gpudb/golui/gpudb-qa/kinetica-python-recordretriever-test; pwd; popd"
            )
        )
    start_task >> parallel_tasks
