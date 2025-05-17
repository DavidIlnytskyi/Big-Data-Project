#!/bin/bash

docker compose up -d --build

bash kafka-population.sh
bash cassandra-population.sh

bash docker exec -i spark spark-submit --master local[1] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/bitnami/spark/data/main.py

bash cassandra-population.sh

bash docker exec -i spark spark-submit --master local[1] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 --conf spark.cassandra.connection.host=cassandra --conf spark.cassandra.auth.username=cassandra --conf spark.cassandra.auth.password=uncommonpass /opt/bitnami/spark/data/submain.py