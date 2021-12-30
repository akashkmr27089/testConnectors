FROM brunneis/python:3.8.3-ubuntu-20.04

# Upgrade installed packages
RUN apt-get update && apt-get upgrade -y && apt-get clean

# (...)

# # Python package management and basic dependencies
RUN apt-get install -y curl
# #RUN  apt-get install -y python3.7


# # Register the version in alternatives
# RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.7

# # Set python 3 as the default python
# RUN update-alternatives --set python /usr/bin/python3.7

# Upgrade pip to latest version
RUN curl -s https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python get-pip.py --force-reinstall && \
    rm get-pip.py


WORKDIR /airbyte/integration_code

COPY setup.py ./
# install necessary packages to a temporary folder
RUN pip install .

# build a clean environment
#FROM base
WORKDIR /airbyte/integration_code

# copy all loaded and built libraries to a pure basic image
#COPY  /install /usr/local
# add default timezone settings
#COPY  /usr/share/zoneinfo/Etc/UTC /etc/localtime
#RUN echo "Etc/UTC" > /etc/timezone

# bash is installed for more convenient debugging.
#RUN apk --no-cache add bash

# copy payload code only
COPY main.py ./
COPY source_pickkr ./source_pickkr

ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=airbyte/source-pickkr
