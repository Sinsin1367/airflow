# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
FROM centos:7
MAINTAINER Sina Asadallahi <sinaa@indeed.com>

ENV TERM linux
ENV AIRFLOW_GPL_UNIDECODE yes
ENV PYTHONPATH=${AIRFLOW_HOME}/config:$PYTHONPATH
ENV AIRFLOW_HOME /var/local/airflow

# Setup dependencies
RUN yum -y update \
    && yum -y groupinstall "Development Tools" \
    && yum -y install \
        vim gcc gcc-c++ cyrus-sasl-devel cyrus-sasl-gssapi libffi-devel \
        libselinux yum-utils mysql mysql-devel fernet python-devel.x86_64 \
	openssl-perl tmux
RUN echo "password" | passwd root --stdin

# Setup python 3
RUN yum -y install https://centos7.iuscommunity.org/ius-release.rpm \
    && yum -y install python36 python36-devel python36-pip \
    wget \
    libczmq-dev \
    curl \
    libssl-dev \
    git \
    libpq-dev \
    inetutils-telnet \
    bind9utils \
    zip \
    unzip \
    gcc \
    && pip3.6 install -U pip \
    && pip3.6 install -U setuptools wheel

RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
        && pip3.6 install -U \
        pytz pyOpenSSL ndg-httpsclient pyasn1 mysqlclient cryptography \
        mysql-connector-python-rf kubernetes celery redis\
        && mkdir -p ${AIRFLOW_HOME}/airflow \
        && mkdir -p ${AIRFLOW_HOME}/dags

# Install kubectl
ARG KUBECTL_VERSION=1.9.3
RUN curl -L -o /usr/local/bin/kubectl https://storage.googleapis.com/kubernetes-release/release/v${KUBECTL_VERSION}/bin/linux/amd64/kubectl \
    && chmod +x /usr/local/bin/kubectl

# Install fixed versions of libraries as 1.10.3 workaround for AIRFLOW-4900
RUN pip install Jinja2==2.10.1 Werkzeug==0.15.4 Flask==1.0.4


COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY airflow_local_settings.py ${AIRFLOW_HOME}/config/

RUN pip install --upgrade pip
COPY requirements.txt /tmp/requirements.txt
#RUN pip install -r /tmp/requirements.txt
COPY airflow.tar.gz /tmp/airflow.tar.gz
RUN pip install --no-use-pep517 /tmp/airflow.tar.gz

COPY airflow-test-env-init.sh ${AIRFLOW_HOME}/airflow-test-env-init.sh

COPY bootstrap.sh ${AIRFLOW_HOME}/bootstrap.sh
RUN chmod +x ${AIRFLOW_HOME}/bootstrap.sh

RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080
USER airflow
WORKDIR ${AIRFLOW_HOME}

ENTRYPOINT ["./bootstrap.sh"]
