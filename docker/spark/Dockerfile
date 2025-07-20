# docker/spark/Dockerfile
FROM bitnami/spark:latest

USER root

# Add custom JARs to Spark's default jars directory
COPY ./jars/*.jar /opt/bitnami/spark/jars/

# (Optional) Change permissions if needed
RUN chmod 644 /opt/bitnami/spark/jars/*.jar

# Install Python libraries
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Revert back to non-root user (default in Bitnami image)
USER 1001
