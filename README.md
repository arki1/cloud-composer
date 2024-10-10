# Cloud Composer Demos

Demo DAGs for developing and testing Airflow DAGs on
Google Cloud Composer.

## Local Development of DAGs

This workflow is based on this documentation
[Reference](https://cloud.google.com/composer/docs/composer-2/run-local-airflow-environments)

The pourpose is to allow a local execution of the simulated
Composer environment in order to facilitate the development of
DAGs as well as testing it's requirements.

### Setup

The `requirements.txt` contains all dependencies
to setup and run a local emulation of the Composer environment.

1. Install some required Debian packages:
```
sudo apt install -yq pkg-config build-essential libmariadb-dev
```
2. Install the Python dependencies in a virtual env:
```
python -m venv venv && \
source venv/bin/activate && \
pip install .
```
3. Create a Composer local env, with the dags path:
```
composer-dev create dev-env \
    --from-image-version composer-2.9.5-airflow-2.9.3 \
    --dags-path ./dags/
```
4. Start the environment and test your DAGs:
```
composer-dev start dev-env
```
5. Export some useful environment variables:
```
source .env
export AIRFLOW_HOME
```

**__Note__**: make sure you have the required configuration
in the `composer/dev-env/variables.env` as well as
the `composer/dev-env/airflow.cfg` files for things
such as the Sendgrid email.

### Debugging

After you have setup the local environment, you can test
DAGs using the local Airflow web server.

Additionally, you can activate the `env` python virtualenv
and run the dags for quick testing and debugging as a regular
Python program:

```
python dags/movielens_etl.py
```
