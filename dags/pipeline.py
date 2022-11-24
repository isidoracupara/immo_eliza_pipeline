from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.email_operator import EmailOperator
from dotenv import load_dotenv

load_dotenv("secrets.env")


default_arg = {
    'owner': 'Team Kafka',
    'start_date': datetime(2022, 11, 17)}

immoweb_dag = DAG('immoweb_workflow', default_args=default_arg,
                  schedule_interval='@daily')

property_scrapping = BashOperator(task_id='get_property', bash_command='python3 /scraping/Immoweb_scrapping.py',
                                  dag=immoweb_dag)

# csv_check = FileSensor(task_id='file_check', filepath='/Users/ahmetsamilcicek/Desktop/becode/pipeline-immoweb-airflow/assets/all_entries.csv', poke_interval=120,
# dag=immoweb_dag)

cleaning_analysis = BashOperator(task_id='clean_dataset', bash_command='python3 /cleaning/cleaning_for_analys.py',
                                 dag=immoweb_dag)

preprocessing_for_prediction = BashOperator(task_id='preprocess_cleaned_data', bash_command='python3 /cleaning/cleaning_for_model.py',
                                            dag=immoweb_dag)

model_prediction = BashOperator(task_id='price_prediction', bash_command='python3 /model_training/model.py',
                                dag=immoweb_dag)

# send_email = EmailOperator(task_id='send_email', to='a.samilcicek@gmail.com', subject='scrapping complete', html_content="Date: {{ ds }}",
# dag=immoweb_dag)

property_scrapping >> cleaning_analysis >> preprocessing_for_prediction >> model_prediction
