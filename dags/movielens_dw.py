from datetime import datetime

from airflow.models import DAG

from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from corp_modules.datasets import MovieLensGCSDataset, MovieLensBigqueryDataset

with DAG(
        dag_id="movielens_dw",
        start_date=datetime(2024, 10, 9),
        schedule=[MovieLensGCSDataset]) as dag:
    
    movies = GCSToBigQueryOperator(
        task_id="movies",
        bucket="{{var.value.movielens_storage_bucket}}",
        source_objects="{{var.value.movielens_storage_path}}/movielens/movies.csv",
        destination_project_dataset_table="ronoaldo.movies",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True)
    
    ratings = GCSToBigQueryOperator(
        task_id="ratings",
        bucket="{{var.value.movielens_storage_bucket}}",
        source_objects="{{var.value.movielens_storage_path}}/movielens/ratings.csv",
        destination_project_dataset_table="ronoaldo.ratings",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True)
    
    dataset = EmptyOperator(
        task_id="dataset",
        outlets=[MovieLensBigqueryDataset])
    
    [movies, ratings] >> dataset


if __name__ == "__main__":
    dag.test()
