from airflow.datasets import Dataset

MovieLensGCSDataset = Dataset("gs://training-gcp-demos-composer/movielens")
MovieLensBigqueryDataset = Dataset("bigquery://ronoaldo.movielens/")