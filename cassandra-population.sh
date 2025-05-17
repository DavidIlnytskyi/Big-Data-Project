echo "Waiting for Cassandra to be ready..."
until docker exec cassandra cqlsh -u cassandra -p uncommonpass -e "DESCRIBE KEYSPACES;" &> /dev/null
do
  sleep 2
done

echo "Cassandra is ready. Creating keyspace and table..."

docker exec -i cassandra cqlsh -u cassandra -p uncommonpass <<EOF
CREATE KEYSPACE IF NOT EXISTS wiki_data
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS wiki_data.results (
    user_id int,
    domain text,
    created_at timestamp,
    page_title text,
    PRIMARY KEY (user_id, created_at)
);
EOF
