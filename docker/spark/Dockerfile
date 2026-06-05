FROM spark:3.5.1

USER root

WORKDIR /opt/ctx-ctr

COPY requirements-docker.txt /tmp/requirements-docker.txt
RUN python3 -m pip install --no-cache-dir pyspark==3.5.1 \
    && python3 -m pip install --no-cache-dir -r /tmp/requirements-docker.txt

USER spark
