FROM mageai/mageai:latest

# Install Java JDK 17
USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk && rm -rf /var/lib/apt/lists/*

# Set environment variables for Java & PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark \
    clickhouse-driver \
    kafka-python \
    confluent_kafka

RUN pip install --upgrade pandas

# Clean up cache for decrease images size
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*

