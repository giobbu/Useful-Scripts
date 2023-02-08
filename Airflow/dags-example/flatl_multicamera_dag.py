from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from random import randint


def camera_tank():
    list_imgs = []
    for i in range(50):
        img = randint(1, 10)
        list_imgs.append(img)
    return list_imgs



def algorithm_detection(ti, identifier):
    list_imgs = ti.xcom_pull(task_ids = f'camera_{identifier}')
    list_anomalies = []
    for img in list_imgs:
        value = img
        if value > 5:
           list_anomalies.append('anomaly')
        else:
           list_anomalies.append('normal')
    return list_anomalies




def alarm_syst(ti, identifier):
    list_anomalies = ti.xcom_pull(task_ids = f'detection_{identifier}')
    count_anomalies = list_anomalies.count("anomaly")    
    if count_anomalies >= int(0.6*len(list_anomalies)):
       message = 'alarm'
    else:
       message = 'not alarm'
    return message



def system_message(ti, n_cameras):

    alarm_syst = [f'alarm_{camera}' for camera in n_cameras]

    list_messages = ti.xcom_pull(task_ids = alarm_syst)

    for message, alarm_module in zip(list_messages, alarm_syst):

        if message == 'alarm':

            print(alarm_module, message)




with DAG("flatl_multicamera_dag", start_date = datetime(2022, 11, 18), schedule_interval = "* * * * *", catchup = False) as dag:
    
    alarm_system = []

    number_cameras = [i for i in range(15)]

    for identifier in number_cameras:

        camera_module = PythonOperator(task_id=f"camera_{identifier}", python_callable = camera_tank)
        
        detection_module = PythonOperator(task_id=f"detection_{identifier}", python_callable = algorithm_detection, op_kwargs={"identifier": identifier})
        
        alarm_module = PythonOperator(task_id=f"alarm_{identifier}", python_callable = alarm_syst, op_kwargs={"identifier": identifier})
    
        camera_module >> detection_module >> alarm_module 

        alarm_system.append(alarm_module)

    message_system = BranchPythonOperator(task_id="system_message", python_callable=system_message, op_kwargs={"n_cameras": number_cameras})

    alarm_system >> message_system


















