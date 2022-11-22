from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Data pipeline for immo eliza',
    'depend_on_past'        : False,
    'start_date'            : datetime(2022, 11, 21),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 1,
    'retry_delay'           : timedelta(minutes=5)
}

with DAG('immo-eliza-pipeline', default_args=default_args, schedule_interval="30 * * * *", catchup=False) as dag:
    dag_start = DummyOperator(
        task_id='dag_start'
        )    
        
    t1 = DockerOperator(
        task_id='scraping',
        image='airflow_scraper:latest',
        container_name='task___scraper',
        api_version='auto',
        auto_remove=True,
        command="echo 'hello'",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
        )

    t2 = DockerOperator(
        task_id='cleaning',
        image='airflow_model:latest',
        container_name='task___model',
        api_version='auto',
        auto_remove=True,
        #command="python3 model/model.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
        )

    t3 = DockerOperator(
        task_id='sql',
        image='airflow_sql:latest',
        container_name='task___sql',
        api_version='auto',
        auto_remove=True,
        #command="python3 sql/sql.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
        )
    
    t4 = DockerOperator(
        task_id='visualization',
        image='airflow_visualization:latest',
        container_name='task___visualization',
        api_version='auto',
        auto_remove=True,
        #command="python3 visualization/visualization.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
        )


    dag_end = BashOperator(
        task_id='dag_end',
        bash_command=f"echo 'end run timestamp: {datetime.now()}' >> /opt/airflow/data/run_log.txt"
        )    

    dag_start >> t1 
    
    t1 >> t2 >> t3 >> t4

    t4 >> dag_end