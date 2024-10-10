import requests
import zipfile
import os
import tempfile
import logging

from datetime import datetime

from airflow.models import DAG
from airflow.utils.task_group import TaskGroup

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLImportInstanceOperator

LOG = logging.getLogger(__name__)

# Default arguments passed to all tasks/dags
ARGS = {
    "start_date": datetime(2024, 10, 9),
}

# File to download
# Reference: https://grouplens.org/datasets/movielens/
FILE = "https://files.grouplens.org/datasets/movielens/ml-latest.zip"

# Local data path
DATA_PATH = "/home/airflow/gcs/data/movielens/"
if __name__ == "__main__":
    # Small hack to allow for easy testing locally
    DATA_PATH = "/tmp/data/movielens/"

def fetch_and_unzip(url, target):
    """
    Downloads a zip file, extracts contents,
    and handles potential seek issues.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        LOG.info(f"Dowloading zip file to {temp_dir}")
        temp_zip = os.path.join(temp_dir, 'temp.zip') 

        # Download the zip file
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            with open(temp_zip, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        LOG.info(f"File downloaded! Extracting to {target}")

        # Now, open the temporary zip file for extraction
        os.makedirs(target, exist_ok=True)
        with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
            zip_ref.extractall(target)
        LOG.info(f"Files extracted into {target}")

def get_zip_basename(file_url):
    """
    Extracts the base name of a .zip file from its URL.
    """
    file_name = os.path.basename(file_url)
    return os.path.splitext(file_name)[0]

def cloud_sql_import(task_id, filename_base):
    """
    Import a CSV file into the configured Cloud SQL Database.

    filename_base is used to calculate both the csv file and
    the table name.
    """
    # Compute the cloud storage URI
    storage_uri = "gs://{{var.value.movielens_storage_bucket}}"
    storage_uri += "/{{var.value.movielens_storage_path}}"
    storage_uri += f"/movielens/{filename_base}.csv"
    # Return the Cloud SQL Operator
    return CloudSQLImportInstanceOperator(
        task_id=task_id,
        instance="my-database",
        body={
            "importContext": {
                "fileType": "csv",
                "uri": storage_uri,
                "database": "{{var.value.movielens_database}}",
                "csvImportOptions": {
                    "table": f"{filename_base}",
                },
            },
        },
        project_id="{{var.value.project_id}}")

with DAG("movielens_etl", default_args=ARGS, schedule=None) as dag:
    download = PythonOperator(
        task_id="download",
        python_callable=fetch_and_unzip,
        op_kwargs={"url": FILE, "target": DATA_PATH})

    ZIP_FOLDER=get_zip_basename(FILE)
    stage = LocalFilesystemToGCSOperator(
        task_id="stage",
        src=f"{DATA_PATH}/{ZIP_FOLDER}/*.csv",
        dst="{{var.value.movielens_storage_path}}/movielens/",
        bucket="{{var.value.movielens_storage_bucket}}")
    
    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rvf {DATA_PATH}"
    )

    with TaskGroup(group_id="cloudsql") as cloudsql:
        load_movies = cloud_sql_import("load_movies", "movies")
        load_ratings = cloud_sql_import("load_ratings", "ratings")
        load_movies >> load_ratings

    notify = EmailOperator(
        task_id="notify",
        conn_id="sendgrid_default",
        to="{{var.value.movielens_notification_email}}",
        subject="[Composer][movielens_etl] Import Success",
        html_content="""<h1>Import Success</h1>
            Movielens data refreshed to the
            <b>{{var.value.movielens_database}}</b>
            Cloud SQL database. Data used from the
            <b>{{var.value.movielens_storage_path}}/movielens/</b>
            folder, that as downloaded from URI: """ + FILE)

    download >> stage >> [cloudsql, cleanup] >> notify

if __name__ == "__main__":
    dag.test()