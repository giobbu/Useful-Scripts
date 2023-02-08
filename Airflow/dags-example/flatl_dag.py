from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from random import randint


def camera_tank():
    list_imgs = []
    for i in range(24):
        img = randint(1, 10)
        list_imgs.append(img)
    return list_imgs



def algorithm_detection(ti):
    list_imgs = ti.xcom_pull(task_ids = 'camera_A')
    list_anomalies = []
    for img in list_imgs:
        value = img
        if value > 5:
           list_anomalies.append('anomaly')
        else:
           list_anomalies.append('normal')
    return list_anomalies



def alarm_syst(ti):
    list_anomalies = ti.xcom_pull(task_ids = ['detection_A'])
    count_anomalies = list_anomalies.count("anomaly")
    if count_anomalies >= 2:
       message = 'alarm triggered'
    else:
       message = 'everything good'
    return message


with DAG("flatl_dag", start_date = datetime(2022, 11, 17), schedule_interval = "* * * * *", catchup = False) as dag:
     
     camera_A = PythonOperator(task_id = 'camera_A', python_callable = camera_tank)
     detection_A = PythonOperator(task_id = 'detection_A', python_callable = algorithm_detection)
     alarm_system = PythonOperator(task_id = 'alarm_system', python_callable = alarm_syst)

camera_A >> detection_A >> alarm_system














