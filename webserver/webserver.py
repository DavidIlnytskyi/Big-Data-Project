from fastapi import FastAPI
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import date, datetime
from typing import List

app = FastAPI()

CASSANDRA_HOST = "cassandra"
KEYSPACE = "wiki_stream"
USERNAME = "cassandra"
PASSWORD = "uncommonpass"

auth_provider = PlainTextAuthProvider(USERNAME, PASSWORD)
cluster = Cluster([CASSANDRA_HOST], auth_provider=auth_provider)
session = cluster.connect(KEYSPACE)


@app.get("/agg_one")
def get_agg_one():
    rows = session.execute("SELECT * FROM agg_one LIMIT 100")
    return [dict(row._asdict()) for row in rows]


@app.get("/agg_two")
def get_agg_two():
    rows = session.execute("SELECT * FROM agg_two LIMIT 100")
    return [dict(row._asdict()) for row in rows]


@app.get("/agg_three")
def get_agg_three():
    rows = session.execute("SELECT * FROM agg_three LIMIT 100")
    return [dict(row._asdict()) for row in rows]


@app.get("/adhoc_one")
def get_adhoc_one():
    rows = session.execute("SELECT * FROM adhoc_one LIMIT 100")
    return [dict(row._asdict()) for row in rows]


@app.get("/adhoc_two")
def get_adhoc_two():
    rows = session.execute("SELECT * FROM adhoc_two LIMIT 100")
    return [dict(row._asdict()) for row in rows]


@app.get("/adhoc_three")
def get_adhoc_two():
    rows = session.execute("SELECT * FROM adhoc_three LIMIT 100")
    return [dict(row._asdict()) for row in rows]


@app.get("/adhoc_four")
def get_adhoc_four():
    rows = session.execute("SELECT * FROM adhoc_four LIMIT 100")
    return [dict(row._asdict()) for row in rows]


@app.get("/adhoc_five")
def get_adhoc_five():
    rows = session.execute("SELECT * FROM adhoc_five LIMIT 100")
    return [dict(row._asdict()) for row in rows]
