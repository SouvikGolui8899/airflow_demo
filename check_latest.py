import os
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
import pendulum
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator


def get_cmd(file_path):
    return open(file_path, 'r').read()


CUR_DIR = os.path.abspath(os.path.dirname(__file__))
cmd = get_cmd(f'{CUR_DIR}/check_latest_cmd.sh')


def choose_branch(host_ip, ti, next_ds):
    install_latest = ti.xcom_pull(task_ids=f"Check_Latest_On_Remote_Host_{host_ip}")
    if install_latest:
        return [f'Install_Latest_On_{host_ip}']
    else:
        return [f'Skip_Install_On_{host_ip}']


with DAG(
    'check_latest',
    default_args={
        'depends_on_past': False,
    },
    description='Check for latest build',
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2022, 4, 20),
    catchup=False,
    tags=['pipeline'],
) as dag:
    start_task = DummyOperator(
        task_id='start_check_latest_task'
    )

    # end_task = DummyOperator(
    #     task_id='end_check_latest_task'
    # )

    parallel_tasks = []
    host_ip_list = ['172.31.1.117']
    for host_ip in host_ip_list:
        hook = SSHHook(
            remote_host=host_ip,
            username='gpudb',
            password='gisfed11'
        )
        check_latest_task = SSHOperator(
            task_id=f"Check_Latest_On_Remote_Host_{host_ip}",
            remote_host=host_ip,
            ssh_hook=hook,
            command=cmd,
            do_xcom_push=True
        )
        branching = BranchPythonOperator(
            task_id='branching',
            python_callable=choose_branch,
            op_kwargs={'host_ip': host_ip}
        )

        install_task = PythonOperator(
            task_id=f'Install_Latest_On_{host_ip}',
            python_callable=lambda: print(f"Installing latest on {host_ip}")
        )

        skip_install_task = DummyOperator(
            task_id=f'Skip_Install_On_{host_ip}'
        )

        parallel_tasks.append(check_latest_task >> branching >> [install_task, skip_install_task])

    start_task >> parallel_tasks
