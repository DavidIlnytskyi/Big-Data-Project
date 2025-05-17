#!/bin/bash

docker compose up -d --build

bash kafka-population.sh
bash cassandra-population.sh
