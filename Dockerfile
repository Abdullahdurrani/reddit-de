FROM apache/airflow:2.6.1-python3.9

USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

WORKDIR /

COPY requirements.txt /

RUN pip install --trusted-host pypi.python.org --default-timeout=100 -r requirements.txt

USER root

RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar --output /home/airflow/.local/lib/python3.9/site-packages/pyspark/jars/hadoop-aws-3.3.4.jar && \
    curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar --output /home/airflow/.local/lib/python3.9/site-packages/pyspark/jars/aws-java-sdk-bundle-1.12.262.jar && \
    curl https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar --output /home/airflow/.local/lib/python3.9/site-packages/pyspark/jars/jets3t-0.9.4.jar  && \
    curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.262/aws-java-sdk-1.12.262.jar --output /home/airflow/.local/lib/python3.9/site-packages/pyspark/jars/aws-java-sdk-1.12.262.jar

USER airflow