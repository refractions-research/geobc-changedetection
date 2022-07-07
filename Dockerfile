
#--------------- BUILD PROJ ------------------------
FROM osgeo/gdal:ubuntu-full-latest

COPY ./changedetection /changedetection/changedetection
COPY LICENSE /changedetection/LICENSE
COPY requirements.txt /changedetection/requirements.txt

RUN apt-get update -y 
RUN apt-get install -y --fix-missing --no-install-recommends apt python3-pip

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get install -y python3.10-tk

WORKDIR /changedetection
RUN pip3 install -r requirements.txt

ENV PROJ_LIB=/usr/share/proj

WORKDIR ./changedetection 