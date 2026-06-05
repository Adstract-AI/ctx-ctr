FROM flink:1.19.1-scala_2.12-java17

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential openjdk-17-jdk python3 python3-dev python3-pip python3-venv \
    && if [ ! -e /opt/java/openjdk/include ]; then ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)/include /opt/java/openjdk/include; fi \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/ctx-ctr

COPY requirements-docker.txt /tmp/requirements-docker.txt
RUN python3 -m pip install --no-cache-dir apache-flink==1.19.1 \
    && python3 -m pip install --no-cache-dir -r /tmp/requirements-docker.txt

USER flink
