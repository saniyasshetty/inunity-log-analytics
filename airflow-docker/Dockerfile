
FROM apache/airflow:3.1.7

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.7 \
    boto3 \
    reportlab
