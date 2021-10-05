# Ubuntu Hirsute gets Python 3.9.5
FROM ubuntu:hirsute

# tzdata setup
ENV TZ America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# python and helpers
RUN apt-get update
RUN apt-get install -y git nano tzdata iputils-ping
RUN apt-get install -y python3 python3-pip

# clone the repo into the docker container
WORKDIR /sillygoose
RUN git clone https://github.com/sillygoose/esphome.git

# install required python packages
WORKDIR /sillygoose/esphome
RUN pip3 install -e .

# add the site-specific configuration/secrets file
WORKDIR /sillygoose/esphome/esphome
ADD .esphome_secrets.yaml .

# run esphome
WORKDIR /sillygoose
CMD ["python3", "esphome/esphome/esphome.py"]