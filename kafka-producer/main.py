from kafka import KafkaProducer
from datetime import datetime, timezone, timedelta
import csv
import time
import aiohttp
import asyncio
import json

TOPIC_NAME = "input"
KAFKA_SERVER = "kafka:19092"

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, api_version=(7, 8))

async def main():
    url = 'https://stream.wikimedia.org/v2/stream/page-create'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            async for line in resp.content:
                line = line.decode('utf-8').strip()
                if line.startswith('data: '):
                    try:
                        data = json.loads(line[6:])

                        producer.send(TOPIC_NAME, json.dumps(data).encode("utf-8"))
                        producer.flush(timeout=1.0)
                        print("Sent")
                    except json.JSONDecodeError:
                        continue

asyncio.run(main())
