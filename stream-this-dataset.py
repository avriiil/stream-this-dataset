import os
import pandas as pd
import sys
import time

from kafka import KafkaProducer

# set kafka topic
topic = "stream-this-dataset"

# set kafka credentials (from upstash)
kafka_endpoint = "talented-cow-10356-eu1-kafka.upstash.io:9092"
kafka_user = os.getenv("UPSTASH_KAFKA_USER")
kafka_pass = os.getenv("UPSTASH_KAFKA_PASS")


# instantiate Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[kafka_endpoint],
    sasl_mechanism="SCRAM-SHA-256",
    security_protocol="SASL_SSL",
    sasl_plain_username=kafka_user,
    sasl_plain_password=kafka_pass,
)


# path to local dataset
local_dataset_path = sys.argv[1]

if ".csv" in local_dataset_path:
    data = pd.read_csv(local_dataset_path)
    print("CSV file loaded in.")
elif ".parquet" in local_dataset_path:
    data = pd.read_parquet(local_dataset_path)
    print("Parquet file loaded in.")

# start timer
start = time.time()

# Send each row as a separate Kafka message
for index, row in data.iterrows():
    message = row.to_json().encode("utf-8")
    producer.send(topic, message)
    time.sleep(0.1)

# end timer
end = time.time()
elapsed = end - start

print("Data sent to Kafka cluster")
print(f"It took {elapsed} to stream this dataset containing {len(data)/1000}K rows")

# close stream
producer.close()
