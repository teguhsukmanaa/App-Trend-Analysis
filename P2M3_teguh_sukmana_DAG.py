'''
=================================================
Milestone 3

Nama  : Teguh Sukmanaputra
Batch : FTDS-029-RMT

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Adapun dataset yang dipakai adalah dataset mengenai aplikasi-aplikasi yang tersedia di Google Playstore.
=================================================
'''

from airflow.models import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.utils.task_group import TaskGroup

import datetime as dt
from datetime import timedelta
from sqlalchemy import create_engine #koneksi ke postgres
import pandas as pd

from elasticsearch import Elasticsearch
# from elasticsearch.helpers import bulk

# def load_csv_to_postgres():
#     '''
#     Fungsi ini bertujuan untuk memasukkan (load) data dari file csv ke sebuah table di PostgreSQL.
#     '''
 
#     database = "airflow_m3"
#     username = "postgres"
#     password = "postgres"
#     host = "postgres"

#     # Membuat URL koneksi PostgreSQL
#     postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

#     # Gunakan URL ini saat membuat koneksi SQLAlchemy
#     engine = create_engine(postgres_url)
#     # engine= create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
#     conn = engine.connect()

#     df = pd.read_csv('/opt/airflow/dags/P2M3_teguh_sukmana_data_raw.csv')
#     #df.to_sql(nama_table_db, conn, index=False, if_exists='replace')
#     df.to_sql('table_m3', conn, index=False, if_exists='replace')
    


def fetch_data_from_postgres():
    '''
    Fungsi ini bertujuan untuk mengambil data dari table di PostgreSQL dan memasukkannya kedalam sebuah file csv.
    '''
    # fetch data
    database = "airflow_m3"
    username = "postgres"
    password = "postgres"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()

    df = pd.read_sql_query("select * from table_m3", conn) #nama table sesuaikan sama nama table di postgres
    df.to_csv('/opt/airflow/dags/P2M3_teguh_sukmana_data_new.csv', sep=',', index=False)
    


def data_preprocessing(): 
    '''
    Fungsi ini bertujuan untuk melakukan preprocessing data/ data cleaning pada file csv dari PostgreSQL yang terbentuk dan memasukkan data hasil cleaning tersebut kedalam file csv baru.
    '''
    # Membaca file csv
    data = pd.read_csv("/opt/airflow/dags/P2M3_teguh_sukmana_data_new.csv")

    # Drop data ke-10472 (dikarenakan informasi data yang tidak sesuai bawaan dari dataset)
    data = data.drop(10472)

    # Drop data duplikat
    data.drop_duplicates(inplace=True)

    # Generalisasi penulisan data pada kolom category
    def clean_category(category):
        '''Fungsi ini merupakan bagian dari rangkaian data cleaning yang bertujuan untuk generalisasi penulisan data pada kolom 'category'
        '''
        return category.lower().replace('_', ' ').title()
    data['Category'] = data['Category'].apply(clean_category)

    # Generalisasi nilai satuan pada kolom size
    def convert_to_kb(size):
        '''
        Fungsi ini merupakan bagian dari rangkaian data cleaning yang bertujuan untuk generalisasi satuan data pada kolom 'size' menjadi ukuran kilobyte
        '''
        if size.endswith('M'):  # Megabytes
            return float(size[:-1]) * 1024
        elif size.endswith('k'):  # Kilobytes
            return float(size[:-1])
        elif size == 'Varies with device':
            return None  # Returning None for "Varies with device"
        else:
            return None  # Handling other cases as missing values   
    data['Size'] = data['Size'].apply(convert_to_kb)

    # Menghilangkan '+' sign and commas dari kolom 'Installs'
    data['Installs'] = data['Installs'].str.replace('+', '').str.replace(',', '')

    # Convert kolom install menjadi numeric type
    data['Installs'] = pd.to_numeric(data['Installs'], errors='coerce')

    # Menghilangkan '$' sign dari kolom 'Installs'
    data['Price'] = data['Price'].str.replace('$', '')

    # Convert kolom 'price' menjadi numeric type
    data['Price'] = pd.to_numeric(data['Price'], errors='coerce')

    # Convert kolom 'reviews' menjadi numeric type
    data['Reviews'] = pd.to_numeric(data['Reviews'], errors='coerce')

    # Convert kolom 'last updated' menjadi datetime type
    data['Last Updated'] = pd.to_datetime(data['Last Updated'])

    # Handling missing value berdasarkan kolom tertentu 'Current Ver', 'Android Ver'
    data = data.dropna(subset=['Current Ver', 'Android Ver'])

    # Normalisasi nama kolom
    data.columns = data.columns.str.lower().str.replace(' ', '_').str.strip()

    # Menyimpan data menjadi csv
    data.to_csv('/opt/airflow/dags/P2M3_teguh_sukmana_data_clean.csv', index=False)
    


def upload_to_elasticsearch():
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/P2M3_teguh_sukmana_data_clean.csv')
    
    for i, r in df.iterrows():
        doc = r.to_json()  # Convert the row to a json
        res = es.index(index="table_m3", doc_type="doc", body=doc)
        print(f"Response from Elasticsearch: {res}")
        

        
default_args = {
    'owner': 'Teguh', 
    'start_date': dt.datetime(2024, 4, 27, 11, 00) - dt.timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=2),
} 

with DAG(
    "P2M3_teguh_sukmana_DAG", #atur sesuai nama project kalian
    description='Milestone_3',
    schedule_interval='30 6 * * *', #atur schedule untuk setiap hari di jam 06:30.
    default_args=default_args, 
    catchup=False
) as dag:
    # # Task : 1
    # load_csv_task = PythonOperator(
    #     task_id='load_csv_to_postgres',
    #     python_callable=load_csv_to_postgres) #sesuaikan dengan nama fungsi yang kalian buat
    
    #task: 2
    ambil_data_pg = PythonOperator(
        task_id='ambil_data_postgres',
        python_callable=fetch_data_from_postgres)

    # Task: 3
    ''' Fungsi ini ditujukan untuk menjalankan pembersihan data.'''
    edit_data = PythonOperator(
        task_id='edit_data',
        python_callable=data_preprocessing)

    # Task: 4
    upload_data = PythonOperator(
        task_id='upload_data_elastic',
        python_callable=upload_to_elasticsearch)

    # #proses untuk menjalankan di airflow
    # load_csv_task >> ambil_data_pg >> edit_data >> upload_data

    #proses untuk menjalankan di airflow
    ambil_data_pg >> edit_data >> upload_data