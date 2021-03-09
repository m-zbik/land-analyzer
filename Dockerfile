# Base container
FROM centos:centos7
# Test comment

# Labels
LABEL image=spark-centos-land_analyzer
LABEL maintainer="Michal Zbikowski <michal.zbikowski@gmail.com>"

USER root

# Env vars
ENV SPARK_VERSION=2.4.5
ENV HADOOP_VERSION=2.7

# Run cmds
# Update yum, install reqs
RUN yum update -y \
    && yum install -y curl \
    && yum install -y bzip2 \
    && yum install -y wget \
    && yum install -y sudo \
    && yum install -y tmux

# Install python 3
RUN yum install -y python3

# Install JDK 8
RUN yum install -y \
    git \
    java-1.8.0-openjdk \
    java-1.8.0-openjdk-devel \
    tar \
    unzip \
    && yum clean all

# Download and install spark

RUN cd /tmp \
    && echo "Downloading and unpacking Spark with Hadoop" \
    && wget --no-verbose http://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /usr/local \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
RUN cd /usr/local && ln -s spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark

# Spark config
ENV SPARK_HOME=/usr/local/spark
ENV SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info" \
    PATH=$PATH:$SPARK_HOME/bin

# Python and anaconda config
ENV PYTHONHASHSEED=0
ENV PYTHONIOENCODING=UTF-8
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# Install miniconda
RUN curl -sSL https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -bfp /usr/local/ \
    && rm -rf /tmp/miniconda.sh \
    && conda install -y python=3.7 \
    && conda update conda \
    && conda clean --all --yes \
    && rpm -e --nodeps curl bzip2 \
    && yum clean all

RUN chmod -R a+rx /usr/local

# Expose ports
EXPOSE 4040

# Create developer user
RUN useradd -ms /bin/bash developer
RUN usermod -aG wheel developer
RUN mkdir /home/developer/land_analyzer
RUN mkdir /home/developer/.ssh
RUN chown -R developer /mnt
RUN chown -R developer /home/developer
RUN chown -R developer /home/developer/.ssh
RUN chmod 700 /home/developer/.ssh

USER developer
# set workdir
WORKDIR /home/developer/land_analyzer

# # Configure conda activate
RUN conda init bash

# COPY and create conda envs
COPY requirements.txt .
COPY requirements-pyspark.txt .

COPY env-local.yml .

RUN conda env create -f env-local.yml
RUN echo "conda activate local" >> ~/.bashrc

RUN /bin/bash -c "source ~/.bashrc"