airflow tasks test parallel_pipeline Runs_On_Remote_Host 2022-04-20

airflow dags trigger --conf '{"hosts": ["172.31.33.33"]}' parallel_pipeline
airflow dags test parallel_pipeline 2022-04-20

airflow dags unpause parallel_pipeline
airflow dags trigger -r '1' parallel_pipeline



