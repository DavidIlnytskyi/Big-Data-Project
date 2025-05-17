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
    hour INT,
    date date,
    timestamp TIMESTAMP,
    PRIMARY KEY ((domain, timestamp), date, hour)
);

CREATE TABLE IF NOT EXISTS wiki_stream.agg_two (
    domain TEXT,
    hour INT,
    date DATE,
    timestamp TIMESTAMP,
    PRIMARY KEY ((domain, timestamp), date, hour)
);

CREATE TABLE IF NOT EXISTS wiki_stream.agg_three (
    user_id BIGINT,
    user_name TEXT,
    timestamp TIMESTAMP,
    date DATE,
    hour INT,
    page_title TEXT,
    PRIMARY KEY ((user_id), timestamp)
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


CREATE TABLE IF NOT EXISTS wiki_stream.adhoc_three (
    domain TEXT,
    timestamp TIMESTAMP,
    url TEXT,
    PRIMARY KEY((domain, url), timestamp)
);

CREATE TABLE IF NOT EXISTS wiki_stream.adhoc_four (
    page_id BIGINT,
    timestamp timestamp,
    url TEXT,
    PRIMARY KEY((page_id, url), timestamp)
);


CREATE TABLE IF NOT EXISTS wiki_stream.adhoc_five (
    user_id BIGINT,
    user_name TEXT,
    timestamp TIMESTAMP,
    PRIMARY KEY ((user_id, timestamp), hour)
);
EOF
