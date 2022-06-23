
#--------------- BUILD PROJ ------------------------
FROM osgeo/gdal:ubuntu-full-latest

COPY ./changedetection /changedectection/changedectection
COPY LICENSE /changedectection/LICENSE
COPY requirements.txt /changedectection/requirements.txt

RUN apt-get update -y 
RUN apt-get install -y --fix-missing --no-install-recommends apt python3-pip

WORKDIR /changedectection
RUN pip3 install -r requirements.txt

ENV PROJ_LIB=/usr/share/proj

WORKDIR ./changedectection 