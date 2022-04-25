try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    #from airflow.operators.bash import BashOperator
    import pandas as pd

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))



def func_hello_world_function_execute():
    print("Hello World..")

# Using XComs to push and Pull Data between the functions..
# https://airflow.apache.org/docs/apache-airflow/stable/concepts/xcoms.html
# Here, we grab the Task Instance.. "ti" itself, which is available in kwargs..

def func_second_function_execute(**kwargs):
    print(" second_function_execute ")
    name = "Vinee - Naveen"
    kwargs['ti'].xcom_push(key='mykey', value="First_function_execute says Hello ")
    kwargs['ti'].xcom_push(key='name', value='Naveen')
    
    name = kwargs['name']

    kwargs['ti'].xcom_push(key='name-2', value=kwargs['name'])

    print('Hello from {}'.format(name))


def func_third_function_execute(**kwargs):
    
    name = kwargs["ti"].xcom_pull(key="name")
    name2 = kwargs["ti"].xcom_pull(key="name-2")

    data = [{"name":"Samm","title":"Data Science Engineer"}, { "name":"Vinee","title":"Devops Engineer"},]
    df = pd.DataFrame(data=data)
    print('@'*66)
    print(df.head())
    print('@'*66)

    print("I am in third_function_execute got value :{} - {} from Function 1  ".format(name, name2))
    
    pass


# DAG Definition
# More Info for Params:
## https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html#it-s-a-dag-definition-file

## Jinja Templater:
## https://jinja.palletsprojects.com/en/3.0.x/templates/

with DAG(
        dag_id="first_dag",
        # We can also use CRON Expression here.. 
        ## https://docs.oracle.com/cd/E12058_01/doc/doc.1014/e12030/cron_expressions.htm
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2022, 4, 1),
        },
        # If Catchup is true, it would compute the delte from start date to current system date, and execute those many jobs.. (according to schedule_interval)
        catchup=False) as f:

    # Task 1
    hello_world_function_execute = PythonOperator(
        task_id="hello_world_function_execute-1",
        python_callable=func_hello_world_function_execute,
        # provide_context=True,
    )

    # Task 2
    second_function_execute = PythonOperator(
        task_id="second_function_execute-2",
        python_callable=func_second_function_execute,
        # Provide Context = True to get the Task Instance in the kwargs..
        provide_context=True,
        op_kwargs={"name":"Vinee"}
    )
    
    # Task 3
    third_function_execute = PythonOperator(
        task_id="third_function_execute-3",
        python_callable=func_third_function_execute,
        # Provide Context = True to get the Task Instance in the kwargs..
        provide_context=True,
    )
    

# Frame the Dependency of Functions..

hello_world_function_execute >> second_function_execute >> third_function_execute