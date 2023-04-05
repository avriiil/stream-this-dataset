# stream-this-dataset
Public streaming datasets can be difficult to find. This repo contains code to convert static (batch) datasets into simulated data streams. This way you can take an existing public dataset and convert it into a streaming use case. 

This code is WIP and currently only supports reading CSV and Parquet datasets. There's much to be improved on here in terms of scalability, parametrization and performance. Feel free to submit PRs (: 


## Dependencies
The script uses `pandas` to access the dataset and send each row as a single streaming message to an Apache Kafka cluster. 

A second example script is included that shows how you could read the data stream from Kafka and analyse it; in this case compiling a list of the locations with the most generous tips. This analysis script uses [`pathway`](www.pathway.com) to perform real-time streaming analysis.


## Sample Datasets
Sample CSV and Parquet files containing rideshare (Uber/Lyft) data from the New York City Taxi and Limousine Commission (TLC) Trip Record* dataset are provided. These are subsampled from the dataset for January 2022. Licensing information for this dataset can be found at https://www.nyc.gov/home/terms-of-use.page.


## A Note on Infrastructure
The Apache Kafka cluster is provisioned through [Upstash](www.upstash.com) which gives you a free, single-replica Kafka cluster. You will need to create an account to run this script yourself. Make sure to set the credentials as environment variables.

Note that the Free-Tier Kafka cluster from Upstash is limited to 10K messages per day, so this is really just for testing purposes. To scale this script for unlimited use, connect to a Kafka cluster you have full control over.
