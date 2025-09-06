from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'Me',
    'start_date': days_ago(0),
    'email': 'email',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL_toll_data',
    'schedule': timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)

unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command='tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1,2,3,4 vehicle-data.csv > csv_data.csv',
    dag=dag,
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5,6,7 tollplaza-data.tsv | tr $"\t"  "," > tsv_data.csv',    
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c 59-71 payment-data.txt | tr " "  "," > fixed_width_data.csv',
    dag=dag,
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='tr "[a-z]" "[A-Z]" extracted_data.csv > transformed_data.csv',
    dag=dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> \
 extract_data_from_fixed_width >> consolidate_data >> transform_data
