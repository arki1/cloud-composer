import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

# Test case
#
# Let's assume current time is 2024-10-14 22:11
# I want to add a new dag to start running at 22h30,
# and keep running every day at 22h30.

# To make easier to understand, lets store the "first execution" as a variable
first_run = pendulum.datetime(2024, 10, 14, 22, 30, tz='America/Sao_Paulo')
# ... note that we pass a timezone, so the schedule + start time are timezone aware:
# https://airflow.apache.org/docs/apache-airflow/2.10.2/authoring-and-scheduling/timezone.html#time-zone-aware-dags 

# Because we want to schedule every day at 22h30, we need to setup a "start_time"
# that will be equals to (first_run - repetition schedule).
# Since our schedule is 'every day', we subtract 1 day to the expected first_run:
start_date = first_run.subtract(days=1)

# Finally, we schedule the DAG to start running every day at:
# 22h30 GMT-3 (because start_time has a timezone)
schedule="30 22 * * *"

# We also set catchup = False to avoid "automatically backfilling" previous intervals.
with DAG(
        dag_id="should_start_at_20241410_2230",
        start_date=start_date,
        schedule=schedule,
        catchup=False,
        is_paused_upon_creation=False,
        ) as dag:
    start = EmptyOperator(task_id='start')
    do_work = EmptyOperator(task_id='do_work')
    end = EmptyOperator(task_id='end')
    start >> do_work >> end
