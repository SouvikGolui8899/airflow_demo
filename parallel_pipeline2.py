import pendulum
from airflow import DAG
from airflow.decorators import dag, task


hosts = []


@task(multiple_outputs=True)
def parse_arguments(**kwargs):
    """
    #### Parse Arguments to the Task
    """
    global hosts
    hosts = eval(kwargs['dag_run'].conf['hosts'])


@task()
def run_task(host_ip):
    f"""
    #### Runs on {host_ip}
    """
    print(f"Running on {host_ip}")


with DAG(
    'parallel_pipeline2',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 4, 20, tz="UTC"),
    catchup=False,
    tags=['pipeline'],
) as dag:
    tasks = []
    for host in hosts:
        tasks.append(lambda: run_task(host))

    parse_arguments.set_downstream(tasks)