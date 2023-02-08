FROM apache/airflow:2.5.0

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY requirements_gpu.txt .
RUN pip install -r requirements_gpu.txt

USER root
RUN apt-get update \
    && apt-get -y install cpulimit gcc git sudo libevent-dev python-dev python3-dev build-essential ffmpeg libsm6 libxext6

WORKDIR /opt/airflow/dags/M5Camera

RUN git clone https://github.com/fizyr/keras-retinanet.git\
    && chmod -R 777 keras-retinanet\
    && mv keras-retinanet keras_retinanet

RUN git clone https://github.com/WongKinYiu/yolov7.git\
    && chmod -R 777 yolov7

USER airflow
RUN pip install --upgrade pip

RUN cd keras_retinanet/\
    && python setup.py build_ext --inplace

RUN pip install git+https://github.com/broadinstitute/keras-resnet\ 
    && pip install Cython tensorflow 

WORKDIR /








