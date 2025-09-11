from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests, tarfile
import pandas as pd

default_args = {
    'owner': 'Me',
    'email': 'email',
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG (
    'ETL_toll_data',
    schedule=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)

def download_dataset():
    url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'
    destination = '/home/project/airflow/dags/python_etl/staging/'

    data = requests.get(url).content

    with open(destination + 'tolldata.tgz', 'wb') as dest_file:
        dest_file.write(data)


def untar_dataset():
    archive_file = '/home/project/airflow/dags/python_etl/staging/tolldata.tgz'

    destination = '/home/project/airflow/dags/python_etl/staging'
    with tarfile.open(archive_file) as input_file:
        input_file.extractall(destination)


def extract_data_from_csv():
    in_csv_file = '/home/project/airflow/dags/python_etl/staging/vehicle-data.csv'

    df = pd.read_csv(in_csv_file)

    df = df.drop(df.columns[[4,5]], axis=1)

    df.to_csv('/home/project/airflow/dags/python_etl/staging/csv_data.csv', index=False)


def extract_data_from_tsv():
    in_tsv_file = '/home/project/airflow/dags/python_etl/staging/tollplaza-data.tsv'

    df = pd.read_table(in_tsv_file)
    df = df[df.columns[[5,6]]]
    df.to_csv('/home/project/airflow/dags/python_etl/staging/tsv_data.csv', index=False)


def extract_data_from_fixed_width():
    in_txt_file = '/home/project/airflow/dags/python_etl/staging/payment-data.txt'
    out_csv_file = '/home/project/airflow/dags/python_etl/staging/fixed_width_data.csv'

    with open(in_txt_file,'r') as infile, open(out_csv_file,'w') as outfile:
        for line in infile:
            outfile.write(line[-10:].replace(' ', ','))


def consolidate_data():
    df = []

    df.append(pd.read_csv('/home/project/airflow/dags/python_etl/staging/csv_data.csv'))
    df.append(pd.read_csv('/home/project/airflow/dags/python_etl/staging/tsv_data.csv'))
    df.append(pd.read_csv('/home/project/airflow/dags/python_etl/staging/fixed_width_data.csv'))

    combined_df = pd.concat(df, axis=1, sort=False)
    combined_df.to_csv('/home/project/airflow/dags/python_etl/staging/extracted_data.csv', index=False)


def transform_data():
    df = pd.read_csv('/home/project/airflow/dags/python_etl/staging/extracted_data.csv')

    df[df.columns[3]] =  df[df.columns[3]].str.upper()

    df.to_csv('/home/project/airflow/dags/python_etl/staging/transformed_data.csv', index=False)


download_dataset = PythonOperator(
    task_id = 'download_dataset',
    python_callable=download_dataset,
    dag=dag,
)

untar_dataset = PythonOperator(
    task_id = 'untar_dataset',
    python_callable=untar_dataset,
    dag=dag,
)

extract_data_from_csv = PythonOperator(
    task_id = 'extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)

extract_data_from_tsv = PythonOperator(
    task_id = 'extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)

extract_data_from_fixed_width = PythonOperator(
    task_id = 'extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

consolidate_data = PythonOperator(
    task_id = 'consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)

transform_data = PythonOperator(
    task_id = 'transform_data',
    python_callable=transform_data,
    dag=dag,
)

download_dataset >> untar_dataset >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
