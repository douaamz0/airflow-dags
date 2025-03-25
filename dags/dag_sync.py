import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Récupérer le token GitHub depuis les variables d'environnement
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")

# URL du repo privé avec authentification
GITHUB_REPO = f"https://{GITHUB_TOKEN}@github.com/douaamz0/airflow-dags.git"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sync_github_dags',
    default_args=default_args,
    description='Synchronisation automatique des DAGs depuis GitHub (privé)',
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    sync_dags = BashOperator(
        task_id='sync_dags',
        bash_command=f"""
        cd /opt/airflow/dags &&
        rm -rf repo &&
        git clone -b main {GITHUB_REPO} repo &&
        cp -r repo/* . &&
        rm -rf repo
        """,
    )

    sync_dags
