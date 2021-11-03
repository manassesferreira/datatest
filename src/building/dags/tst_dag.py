from airflow import DAG
from airflow.utils.dates import days_ago, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from tst import Test

with DAG(
    'dataTest',
    default_args={'owner': 'airflow'},
    description='Enumeration and comparation data test',
    schedule_interval="*/22 * * * *",
    dagrun_timeout=timedelta(minutes=31),
    start_date=days_ago(0,10,20,0,0),
    end_date=days_ago(0,23,59,0,0),
    tags=['dataTest'],
) as dag:

    _begin = DummyOperator(task_id='begin')

    _get_test = PythonOperator(
        task_id='get_test',
        python_callable=Test.get_test,
        dag=dag
    )

    _collect_target = PythonOperator(
        task_id='collect_target',
        python_callable=Test.collect,
        op_kwargs={'fonte': 'destino' },
        dag=dag
    )

    _collect_source = PythonOperator(
        task_id='collect_source',
        python_callable=Test.collect,
        op_kwargs={'fonte': 'origem' },
        dag=dag
    )

    _assert_and_save_test = PythonOperator(
        task_id='assert_and_save_test',
        python_callable=Test._assert_and_save_test,
        dag=dag
    )   

    _end = DummyOperator(
        task_id='end',
        trigger_rule="none_skipped",
    )

    _begin >> _get_test
    _get_test >> _collect_source
    _get_test >> _collect_target
    _collect_source >> _assert_and_save_test
    _collect_target >> _assert_and_save_test
    _assert_and_save_test >> _end
