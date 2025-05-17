echo "Waiting for Cassandra to be ready..."
until docker exec cassandra cqlsh -u cassandra -p uncommonpass -e "DESCRIBE KEYSPACES;" &> /dev/null
do
  sleep 2
done

echo "Cassandra is ready. Creating keyspace and table..."

docker exec -i cassandra cqlsh -u cassandra -p uncommonpass <<EOF
CREATE KEYSPACE IF NOT EXISTS wiki_stream WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS wiki_stream.agg_one (
    domain TEXT,
    date DATE,
    hour INT,
    count COUNTER,
    PRIMARY KEY ((domain, date, hour))
);

CREATE TABLE IF NOT EXISTS wiki_stream.agg_two (
    domain TEXT,
    date DATE,
    hour INT,
    count COUNTER,
    PRIMARY KEY ((domain, date, hour))
);


CREATE TABLE IF NOT EXISTS wiki_stream.agg_three (
    user_id BIGINT,
    user_name TEXT,
    date DATE,
    hour INT,
    timestamp TIMESTAMP,
    page_title TEXT,
    PRIMARY KEY ((user_id, date, hour), timestamp)
);

CREATE TABLE IF NOT EXISTS wiki_stream.adhoc_one (
    domain TEXT,
    count COUNTER,
    PRIMARY KEY (domain)
);

CREATE TABLE IF NOT EXISTS wiki_stream.adhoc_two (
    user_id BIGINT,
    timestamp TIMESTAMP,
    url TEXT,
    PRIMARY KEY(user_id, timestamp)
);

CREATE TABLE IF NOT EXISTS wiki_stream.adhoc_four (
    page_id BIGINT,
    timestamp timestamp,
    url TEXT,
    PRIMARY KEY(page_id, timestamp)
);

CREATE TABLE IF NOT EXISTS wiki_stream.adhoc_five (
    user_id BIGINT,
    user_name TEXT,
    hour INT,
    date DATE,
    timestamp TIMESTAMP,
    PRIMARY KEY ((user_id, timestamp), hour)
);
EOF
