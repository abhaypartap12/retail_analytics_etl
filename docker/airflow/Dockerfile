FROM apache/airflow:2.5.0

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    libcurl4-openssl-dev \
    zlib1g-dev \
    wget \
    build-essential \
    python3-dev \
    default-libmysqlclient-dev \
    libsasl2-dev \
    libssl-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install librdkafka
RUN wget https://github.com/edenhill/librdkafka/archive/v1.9.2.tar.gz && \
    tar -xzf v1.9.2.tar.gz && \
    cd librdkafka-1.9.2 && \
    ./configure && \
    make && \
    make install && \
    ldconfig && \
    cd .. && \
    rm -rf librdkafka-1.9.2 v1.9.2.tar.gz

# Install Spark 3.5.1
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$SPARK_HOME/bin

RUN curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Switch back to airflow user
USER airflow

# Install Python libraries
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAGs, scripts, data
COPY ./dags /opt/airflow/dags
COPY ./scripts /opt/airflow/scripts
COPY ./data /opt/airflow/data
