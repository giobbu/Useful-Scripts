from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from random import randint


# define model training
def training_model():
    return randint(1, 10)


# select best model accuracy
def select_model(ti):
    accuracies = ti.xcom_pull(task_ids = ['training_model_A', 'training_model_B', 'training_model_C'])
    best_accuracy = max(accuracies)
    if (best_accuracy > 0.8):
       return 'accurate_model'
    else:
       return 'inaccurate_model'


# context manager DAG
with DAG("my_dag", start_date = datetime(2022, 11, 17), schedule_interval = "* * * * *", catchup = False) as dag:

         model_A = PythonOperator(task_id = 'training_model_A', python_callable = training_model)
         model_B = PythonOperator(task_id = 'training_model_B', python_callable = training_model)
         model_C = PythonOperator(task_id = 'training_model_C', python_callable = training_model)

         select_best_model = BranchPythonOperator(task_id = 'select_model', python_callable = select_model)

         accurate_model = BashOperator(task_id = 'accurate_model', bash_command = "echo 'the best model is accurate'")
         inaccurate_model = BashOperator( task_id = 'inaccurate_model', bash_command = "echo 'the best model is inaccurate'")

[model_A, model_B, model_C] >> select_best_model >> [accurate_model, inaccurate_model]
