FROM flink:1.17.2-scala_2.12-java11

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN python -m pip install apache-flink psycopg2-binary

COPY flink_job.py /opt/flink/

WORKDIR /opt/flink