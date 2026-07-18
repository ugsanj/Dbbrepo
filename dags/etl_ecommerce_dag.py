"""
Airflow DAG: ETL E-Commerce Orders
File ini akan diletakkan di folder dags/ Apache Airflow.
Airflow akan otomatis mendeteksi dan menjadwalkannya.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# =====================================================================
# 1. TEMPATKAN FUNGSI EKSKUSI DI SINI (Agar tidak NameError)
# =====================================================================
def extract_from_source():
    print("Ekstraksi data transaksi e-commerce selesai.")

def transform_data():
    print("Data cleansing & feature engineering selesai.")

def validate_data():
    print("Data quality check passed. Tidak ada anomali.")

def load_to_bigquery():
    print("Data berhasil dimuat ke target data warehouse.")

def generate_summary():
    print("Laporan performansi bisnis bulanan berhasil dibuat.")

def send_slack_alert():
    print("Notifikasi status pipeline berhasil dikirim.")

# === DEFAULT ARGS ===
# Konfigurasi yang berlaku untuk semua task
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email': ['alert@company.com'],
    'email_on_failure': True,         # Kirim email jika gagal
    'email_on_retry': False,
    'retries': 3,                     # Coba ulang 3x jika gagal
    'retry_delay': timedelta(minutes=5),  # Jeda 5 menit antar retry
}

# === DAG DEFINITION ===
with DAG(
    dag_id='etl_ecommerce_daily',
    default_args=default_args,
    description='Daily ETL pipeline untuk data transaksi e-commerce',
    schedule='0 6 * * *',             # Jalan setiap hari jam 06:00 WIB
    start_date=datetime(2024, 1, 1),
    catchup=False,                    # Jangan jalankan untuk hari yang terlewat
    tags=['etl', 'ecommerce', 'daily'],
) as dag:

    # Task 1: Start marker
    start = EmptyOperator(task_id='start')

    # Task 2: Extract data dari source
    extract = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_from_source,  # fungsi dari module terpisah
    )

    # Task 3: Transform & clean data
    transform = PythonOperator(
        task_id='transform_and_clean',
        python_callable=transform_data,
    )

    # Task 4: Validate data quality
    validate = PythonOperator(
        task_id='validate_quality',
        python_callable=validate_data,
    )

    # Task 5: Load ke warehouse
    load = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=load_to_bigquery,
    )

    # Task 6: Generate report
    report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_summary,
    )

    # Task 7: Send notification
    notify = PythonOperator(
        task_id='send_notification',
        python_callable=send_slack_alert,
    )

    # Task 8: End marker
    end = EmptyOperator(task_id='end')

    # === TASK DEPENDENCIES ===
    # Ini yang mendefinisikan urutan (DAG = Directed Acyclic Graph)
    start >> extract >> transform >> validate >> load >> report >> notify >> end
