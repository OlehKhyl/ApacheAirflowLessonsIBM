import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

download_link = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt'
downloaded_data = 'downloaded_data.txt'
transformed_data = 'transformed_data.txt'
output_file = 'server_access_logs.csv'

def extract():

    data_from_link = requests.get(download_link).text

    with open(downloaded_data,'w') as outfile:
        outfile.write(data_from_link)


def transform():
    with open(downloaded_data,'r') as infile, open(transformed_data,'w') as outfile:

        header_line = infile.readline().split("#")
        outfile.write(header_line[0] + ',' + header_line[3] + '\n')

        for line in infile:
            columns = line.split("#")
            timestamp = columns[0]
            visitorid = columns[3].upper()

            outfile.write(timestamp + ',' + visitorid + '\n')


def load():

    with open(transformed_data,'r') as infile, open(output_file,'w') as outfile:
        for line in infile:
            outfile.write(line)


default_args = {
    'owner': 'me',
    'start_date': days_ago(0),
    'email': ['email'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL_Server_Access_Log_Processing',
    default_args = default_args,
    description = 'Downloads server access logs from link and saves timestamp and visitorid in CSV',
    schedule_interval=timedelta(hours=24),
)

execute_extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    dag = dag,
)

execute_transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    dag = dag,
)

execute_load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    dag = dag,
)

execute_extract >> execute_transform >> execute_load
