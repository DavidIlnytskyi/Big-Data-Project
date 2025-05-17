docker exec kafka kafka-topics --bootstrap-server localhost:9092 --topic input --create --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --topic processed --create --partitions 3 --replication-factor 1
