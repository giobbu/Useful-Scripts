FROM ubuntu

# args for uid and password
ARG UID
ARG PASSWORD

RUN apt-get -y update;
RUN apt-get -y clean all;

# Install basic tools
RUN apt-get install -y wget curl sudo lsof vim nano bzip2 passwd tar bc git unzip python3-pip

# Install requirements for real-time
COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

# open-cv headless only cpu
RUN pip install opencv-python-headless

ENV HOME /home/app
WORKDIR $HOME

RUN useradd -rm -d /home/app/data  -s /bin/sh guest -u $UID
RUN echo guest:$PASSWORD | chpasswd 
USER guest

COPY /daily_detect_cluster ./daily_detect_cluster










#RUN mkdir /home/app/data

# Create guest user
#RUN useradd -rm -d /home/app/data  -s /bin/sh guest -u $UID
#RUN echo guest | passwd guest --stdin
#USER guest





# BUILD: docker build --build-arg UID=$(id -u) -t giobbu/... .














