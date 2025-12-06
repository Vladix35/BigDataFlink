FROM python:3.12

WORKDIR /app

COPY producer.py data.json .

RUN pip install confluent-kafka

CMD ["sh", "-c", "echo sleep 30 && python producer.py"]