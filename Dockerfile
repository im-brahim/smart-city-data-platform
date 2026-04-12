FROM bitnami/spark:3.5.0

USER root

# Copy custom JARs to the official Spark jars directory
COPY ./jars/*.jar /opt/bitnami/spark/jars/

# Install pip packages
COPY ./jobs/utils/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm -rf /root/.cache /tmp/*

USER 1001