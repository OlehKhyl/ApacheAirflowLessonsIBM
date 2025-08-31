from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'me',
    'email': ['email'],
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL_Server_Access_Log_Processing_BASH',
    default_args = default_args,
    description= 'Server_Access_Log_Processing_BASH',
    schedule_interval = timedelta(days=1),
)

download = BashOperator(
    task_id = 'download',
    bash_command = 'curl "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt" > -o web-server-access-log.txt',
    dag = dag,
)

extract = BashOperator(
    task_id = 'extract',
    bash_command = 'cut -d"#" -f1,4 < web-server-access-log.txt > extracted.txt',
    dag = dag,
)

transform = BashOperator(
    task_id = 'transform',
    bash_command = 'tr "[a-z]" "[A-Z]" < extracted.txt > transformed.txt',
    dag = dag,
)

load = BashOperator(
    task_id = 'load',
    bash_command = 'zip log.zip transformed.txt',
    dag = dag,
)

download >> extract >> transform >> load
