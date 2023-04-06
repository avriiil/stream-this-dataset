import os
import pathway as pw

# set kafka credentials (from upstash)
kafka_endpoint = "talented-cow-10356-eu1-kafka.upstash.io:9092"
kafka_user = os.getenv("UPSTASH_KAFKA_USER")
kafka_pass = os.getenv("UPSTASH_KAFKA_PASS")

# define kafka cluster settings
rdkafka_settings = {
    "bootstrap.servers": kafka_endpoint,
    "security.protocol": "sasl_ssl",
    "sasl.mechanism": "SCRAM-SHA-256",
    "group.id": "$GROUP_NAME",
    "session.timeout.ms": "6000",
    "sasl.username": kafka_user,
    "sasl.password": kafka_pass,
}

# use kafka connector to read the kafka stream
input_trips = pw.kafka.read(
    rdkafka_settings,
    topic_names=["stream-this-dataset"],
    value_columns=[
        "PULocationID",
        "DOLocationID",
        "trip_miles",
        "trip_time",
        "base_passenger_fare",
        "tips",
        "driver_pay",
    ],
    format="json",
    autocommit_duration_ms=1000,
    types={
        "PULocationID": pw.Type.INT,
        "DOLocationID": pw.Type.INT,
        "trip_miles": pw.Type.FLOAT,
        "trip_time": pw.Type.FLOAT,
        "base_passenger_fare": pw.Type.FLOAT,
        "tips": pw.Type.FLOAT,
        "driver_pay": pw.Type.FLOAT,
    },
)

# remove entries with driver_pay = 0 and tips = 0
input_trips = input_trips.filter(pw.this.driver_pay > float(0))
input_trips = input_trips.filter(pw.this.tips > float(0))

# create column with tip as percentage of driver_pay
tip_trips = input_trips.select(
    *pw.this,
    tip_percentage=pw.this.tips / pw.this.driver_pay * float(100),
)

# filter out generous tips (>= 30% of driver_pay)
generous_trips = tip_trips.filter(pw.this.tip_percentage > float(30))

# use generous trips to get a list of PULocations
# where each row is a generous PULocationID with the tip_percentage
locations = generous_trips.groupby(pw.this.PULocationID).reduce(
    pw.this.PULocationID,
    tip_percentage=pw.reducers.sorted_tuple(pw.this.tip_percentage),
)

# output results to CSV file
pw.csv.write(locations, "./output-high-tip-locations.csv")

# run the pathway engine
pw.run()
