import sys
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
import pendulum
from airflow import DAG

# Operators; we need this to operate!
from airflow.models import XCom
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.session import provide_session
from sqlalchemy import desc

dag = DAG(
    'parallel_pipeline',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='Parallel Pipeline',
    schedule_interval=None,
    start_date=datetime(2022, 4, 20),
    catchup=False,
    tags=['pipeline'],
)


def parse_args(**kwargs):
    host_list = kwargs["dag_run"].conf['hosts']  # here we get the parameters we specify when triggering
    return host_list


# def generate_tasks(**kwargs):
#     ti = kwargs['ti']
#     host_list = ti.xcom_pull(task_ids='parse_args')
#     parallel_task_list = []
#     for host_ip in host_list:
#         hook = SSHHook(
#             remote_host=host_ip,
#             username='gpudb',
#             password='gisfed11'
#         )
#         parallel_task_list.append(
#             SSHOperator(
#                 task_id=f"Runs_On_Remote_Host_{host_ip}",
#                 remote_host=host_ip,
#                 ssh_hook=hook,
#                 command='hostname -i',
#                 dag=dag
#             )
#         )


parse_arg_task = PythonOperator(
    task_id='parse_args',
    python_callable=parse_args,
    dag=dag
)


@provide_session
def get_sqs_messages(session):
    # xcom = XCom.get_one(execution_date=pendulum.datetime(2022, 4, 21),
    #              key='return_value',
    #              dag_id='parallel_pipeline',
    #              session=session)
    xcoms = session.query(XCom).filter(
        XCom.dag_id == 'parallel_pipeline', XCom.task_id == 'parse_args').order_by(desc(XCom.execution_date)).first()
    # ensure the most recent value is retrieved.
    # query = query.order_by("execution_date desc")
    if xcoms:
        return xcoms.value
    else:
        return []


# generation_task = PythonOperator(
#     task_id='generate_task',
#     python_callable=generate_tasks,
#     dag=dag
# )
#
# parse_arg_task.set_downstream(generation_task)
# generation_task.set_downstream(parallel_task_list)
parallel_tasks = []

for host_ip in get_sqs_messages():
    hook = SSHHook(
        remote_host=host_ip,
        username='gpudb',
        password='gisfed11'
    )
    parallel_tasks.append(
        SSHOperator(
            task_id=f"Runs_On_Remote_Host_{host_ip}",
            remote_host=host_ip,
            ssh_hook=hook,
            command='hostname -i',
            dag=dag
        )
    )

parse_arg_task.set_downstream(parallel_tasks)
