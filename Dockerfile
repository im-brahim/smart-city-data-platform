FROM apache/spark:3.5.0

USER root

# Copy custom JARs to the official Spark jars directory
COPY ./jars/*.jar /opt/my/spark/jars/

# Install pip packages
COPY ./jobs/utils/requirements.txt /tmp/requirements.txt


RUN set -ex && \
    apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm -rf /root/.cache /tmp/*

USER spark