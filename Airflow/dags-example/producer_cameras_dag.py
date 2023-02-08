from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import os
from datetime import datetime
from random import randint
import numpy as np
import cv2 as cv

NUM_CAMERAS = 5
TMP_FILE = Dataset('/opt/airflow/dags/datastore/tmp/')
DATE_TIME = datetime.now().strftime("%m-%d-%Y_%H-%M-%S")

# trigger batch receiver
trigger_batch_receiver = TriggerDagRunOperator(
                                                task_id = 'trigger_camera_system_receiver',
                                                trigger_dag_id = 'batch_camera_system_receiver',
                                                execution_date = '{{ ds }}',
                                                reset_dag_run=True,
                                                wait_for_completion=True,
                                                poke_interval = 30
                                                )


with DAG(dag_id='camera_system_producer',
         start_date = datetime(2022, 11, 21), 
         schedule = '39-47 * * * *',
         catchup = False) as dag:


    @task(outlets=[TMP_FILE])
    def camera_tank(id_camera):

        CAMERA_FILE = TMP_FILE.uri + 'camera_' + str(id_camera)
        if not os.path.exists(CAMERA_FILE):
            os.makedirs(CAMERA_FILE)

        # create image
        IMG = []
        for i in range(10):
            img = randint(1, 10)
            IMG.append(img)
        
        # save image in camera folder 
        with open(CAMERA_FILE + '/' + DATE_TIME +'.npy', 'wb') as f:
            np.save(f, np.array(IMG))


    # replicate operation for the total number of cameras
    for camera_id in range(NUM_CAMERAS):  camera_tank(camera_id) >> trigger_batch_receiver 

        
























