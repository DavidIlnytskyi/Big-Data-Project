from fastapi import FastAPI
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import date, datetime, timezone, timedelta
from typing import List
import pandas as pd
import json

app = FastAPI()

CASSANDRA_HOST = "cassandra"
KEYSPACE = "wiki_stream"
USERNAME = "cassandra"
PASSWORD = "uncommonpass"

auth_provider = PlainTextAuthProvider(USERNAME, PASSWORD)
cluster = Cluster([CASSANDRA_HOST], auth_provider=auth_provider)
session = cluster.connect(KEYSPACE)

def get_time_borders():
    def format_datetime(dt):
        return dt.strftime("%Y-%m-%d,%H").split(",")

    now = datetime.now(timezone.utc)
    current_hour = now.replace(minute=0, second=0, microsecond=0)

    start_time = current_hour - timedelta(hours=7)
    end_time = current_hour - timedelta(hours=1)
    start_date, start_hour = format_datetime(start_time)
    end_date, end_hour = format_datetime(end_time)
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    return start_date, end_date, int(start_hour), int(end_hour)

def filter_df_by_time(df):
    sd, ed, sh, eh = get_time_borders()

    a = ((df["date"].apply(lambda x: x.date()) == sd) & (df["hour"] >= sh))
    b = ((df["date"].apply(lambda x: x.date()) == ed) & (df["hour"] < eh))

    filtered_df = df[a & b] if sd == ed else df[a | b]
    return filtered_df

def agg_one_or_two(table):
    rows = session.execute(f"SELECT * FROM {table}")
    df = pd.DataFrame(rows.all())

    filtered_df = filter_df_by_time(df)
    grouped = filtered_df.groupby('hour')

    result = []
    for hour, group in grouped:
        time_start = f"{hour:02}:00"
        time_end = f"{(hour + 1) % 24:02}:00"
        stats = [{row['domain']: row['count']} for _, row in group.iterrows()]
        result.append({
            "time_start": time_start,
            "time_end": time_end,
            "statistics": stats
        })

    agg_json = json.dumps(result, indent=2)

    return agg_json


@app.get("/agg_one")
def get_agg_one():
    agg_one_json = agg_one_or_two("agg_one")
    return agg_one_json


@app.get("/agg_two")
def get_agg_two():
    agg_one_json = agg_one_or_two("agg_two")
    return agg_one_json


@app.get("/agg_three")
def get_agg_three():
    rows = session.execute("SELECT * FROM agg_three")
    df = pd.DataFrame(rows.all())

    filtered_df = filter_df_by_time(df)

    grouped = filtered_df.groupby(['user_name', 'user_id'])

    result = grouped.agg(
        start_time=('timestamp', 'min'),
        end_time=('timestamp', 'max'),
        pages_created=('page_title', 'count'),
        page_titles=('page_title', list)
    ).reset_index()

    agg_three_json = json.dumps(json.loads(result.sort_values(by='pages_created', ascending=False).head(20).to_json(orient='records', date_format='iso')), indent=2)

    return agg_three_json

@app.get("/adhoc_one")
def get_adhoc_one():
    rows = session.execute("SELECT * FROM adhoc_one")
    df = pd.DataFrame(rows.all())
    adhoc_one_json = {"domains" : list(set(df["domain"].to_list()))}

    return adhoc_one_json


@app.get("/adhoc_two")
def get_adhoc_two(user_id: int):
    rows = session.execute("SELECT * FROM adhoc_two")
    df = pd.DataFrame(rows.all())

    adhoc_two_json = {
        "user_id": user_id,
        "created_pages": df[df['user_id'] == user_id]["url"].to_list()
    }

    return adhoc_two_json


@app.get("/adhoc_three")
def get_adhoc_two(specified_domain: str):

    rows = session.execute("SELECT * FROM adhoc_one")
    df = pd.DataFrame(rows.all())

    adhoc_three_json = {"domain": specified_domain, "count" : int(df[df["domain"] == specified_domain]["count"].iloc[0])}

    return adhoc_three_json


@app.get("/adhoc_four")
def get_adhoc_four(page_id: int):
    rows = session.execute("SELECT * FROM adhoc_four")
    df = pd.DataFrame(rows.all())

    adhoc_four_json = {"page_id": page_id, "url": df[df["page_id"] == page_id]["url"][0]}

    return adhoc_four_json


@app.get("/adhoc_five")
def get_adhoc_five(start_date: str, end_date: str):
    rows = session.execute("SELECT * FROM adhoc_five")
    df = pd.DataFrame(rows.all())

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    start_time = pd.Timestamp(start_date)
    end_time = pd.Timestamp(end_date)

    filtered = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]

    result = filtered.groupby(['user_id', 'user_name']).size().reset_index(name='pages_created')

    adhoc_five_json = json.dumps(json.loads(result.to_json(orient='records', date_format='iso')), indent=2)

    return adhoc_five_json
