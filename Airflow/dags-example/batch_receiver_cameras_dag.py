from airflow import DAG, Dataset
from airflow.decorators import task


from datetime import datetime
import subprocess
import os
import numpy as np


NUM_CAMERAS = 5
DATE_TIME = datetime.now().strftime("%m-%d-%Y_%H-%M-%S")
TMP_FILE = '/opt/airflow/dags/datastore/tmp/'
IMG_FILE = '/opt/airflow/dags/datastore/batch_camera_results/'
DAILY_FILES = 4

TMP_FILE_DATASET = Dataset(TMP_FILE)
if not os.path.exists(IMG_FILE): os.makedirs(IMG_FILE)
IMG_AIRFLOW_DATASET = Dataset(IMG_FILE)



param = {
   'NUM_CAMERAS' : 5,
   'DATE_TIME' : datetime.now().strftime("%m-%d-%Y_%H-%M-%S"),
   'TMP_FILE' : '/opt/airflow/dags/datastore/tmp/',
   'IMG_FILE' : '/opt/airflow/dags/datastore/batch_camera_results/',
   'DAILY_FILES' : 4
}


with DAG(dag_id = 'batch_camera_system_receiver',
         start_date = datetime(2022, 11, 21), 
         schedule = [TMP_FILE_DATASET],
         catchup = False) as dag: 
         
         @task(outlets=[IMG_AIRFLOW_DATASET])
         def data_acquisition(id_camera):

             CAMERA_PATH = TMP_FILE.uri + 'camera_' + str(id_camera) + '/'

             list_files = ("ls " + CAMERA_PATH).split()

             ps = subprocess.Popen(list_files, stdout=subprocess.PIPE)
             stdout, _ = ps.communicate()
             list_name_files = stdout.decode("utf-8").split("\n")[:-1]

             if len(list_name_files) > DAILY_FILES:
                
                for name_file in list_name_files:

                     # print last image in camera folder 
                     print(CAMERA_PATH + name_file)

                     # load image as numpy array
                     IMG = np.load(CAMERA_PATH + name_file)

                     # save image in camera folder 
                     with open(IMG_FILE + 'camera_' + str(id_camera) + '_' + name_file, 'wb') as f:
                        np.save(f, IMG)

             else:
                print('NOT ENOUGH FILES')

         for camera_id in range(NUM_CAMERAS):
            data_acquisition(camera_id)
