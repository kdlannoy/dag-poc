from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'git-test-jenkins-DAG',
    default_args=default_args,
    description='Querying a postgres db',
    schedule_interval=timedelta(days=1),
)

timestamp = """{{ ts }}"""

do_query = \
    """
    INSERT INTO ENGLISH_ACTOR_COUNTER (
    timestamp,
    count)
    VALUES (
    'now()'::timestamp,
    (SELECT count(*)
FROM film
         INNER JOIN film_actor fa on film.film_id = fa.film_id
         INNER JOIN actor a on fa.actor_id = a.actor_id
         INNER JOIN inventory i on film.film_id = i.film_id
         INNER JOIN language l on film.language_id = l.language_id
WHERE l.name = 'English')
    )
    
    """.format()

create_table = \
    """
    CREATE TABLE IF NOT EXISTS ENGLISH_ACTOR_COUNTER (
    timestamp varchar(100) NOT NULL,
    count integer NOT NULL
    )
    """.format()

create_table_op = PostgresOperator(
    task_id='create_table_if_not_exists',
    sql=create_table,
    postgres_conn_id='POSTGRES_DATA',
    dag=dag
)

create_open_opp = PostgresOperator(
    task_id='create_open_opp',
    sql=do_query,
    postgres_conn_id='POSTGRES_DATA',
    dag=dag)

create_table_op >> create_open_opp
