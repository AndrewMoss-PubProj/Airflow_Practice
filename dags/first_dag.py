try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators import PythonOperator
    from datetime import datetime
    import pandas as pd
    import datapackage

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def first_function_execute(**context):
    print("first_function_execute   ")


    data_url = 'https://datahub.io/five-thirty-eight/nfl-elo/datapackage.json'

    # to load Data Package into storage
    package = datapackage.Package(data_url)
    elos = pd.DataFrame()
    # to load only tabular data
    resources = package.resources
    for resource in resources:
        print('fetching resource')
        if resource.tabular:
            elos = pd.read_csv(resource.descriptor['path'])
            break
    context['ti'].xcom_push(key='mykey', value=elos)


def second_function_execute(**context):
    elos = context.get("ti").xcom_pull(key="mykey")
    print(elos.head(5))


with DAG(
        dag_id="first_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=10),
            "start_date": datetime(2023, 1, 1),
        },
        catchup=False) as f:

    first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute,
        provide_context=True,
    )

    second_function_execute = PythonOperator(
        task_id="second_function_execute",
        python_callable=second_function_execute,
        provide_context=True,
    )

first_function_execute >> second_function_execute
