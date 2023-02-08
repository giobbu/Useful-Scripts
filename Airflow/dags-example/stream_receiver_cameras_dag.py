from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime
import subprocess
import os
import numpy as np


NUM_CAMERAS = 5
TMP_FILE = Dataset('/opt/airflow/dags/datastore/tmp/')
DATE_TIME = datetime.now().strftime("%m-%d-%Y_%H-%M-%S")
IMG_FILE = '/opt/airflow/dags/datastore/stream_camera_results/'

if not os.path.exists(IMG_FILE): os.makedirs(IMG_FILE)
IMG_AIRFLOW_DATASET = Dataset(IMG_FILE)

with DAG(dag_id = 'stream_camera_system_receiver',
         start_date = datetime(2022, 11, 21), 
         schedule = [TMP_FILE],
         catchup = False) as dag: 
         
         @task(outlets=[IMG_AIRFLOW_DATASET])
         def data_acquisition(id_camera):
             CAMERA_PATH = TMP_FILE.uri + 'camera_' + str(id_camera) + '/'

             list_files = ("ls " + CAMERA_PATH).split()
             name_last_file = (" tail -n 1").split()

             ps = subprocess.Popen(list_files , stdout=subprocess.PIPE)
             output = subprocess.check_output(name_last_file, stdin=ps.stdout)
             recent_img = output.decode("utf-8").split("\n")[0]

             # print last image in camera folder 
             print(CAMERA_PATH + recent_img)

             # load image as numpy array
             IMG = np.load(CAMERA_PATH + recent_img)

             # save image in camera folder 
             with open(IMG_AIRFLOW_DATASET.uri + 'camera_' + str(id_camera) +'_' + DATE_TIME +'.npy', 'wb') as f:
                np.save(f, IMG)

         for camera_id in range(NUM_CAMERAS):
            data_acquisition(camera_id)
