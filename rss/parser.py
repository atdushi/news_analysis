#!/usr/bin/python3

import requests as req
from bs4 import BeautifulSoup, CData, Tag
import time
import tqdm
import pickle
import pandas as pd
import json
import fire

sites = {
    'Фонтанка.ру': ['https://www.fontanka.ru/fontanka.rss', ''],
    'LENTA.RU': ['https://lenta.ru/rss', ''],
    'TASS': ['https://tass.ru/rss/v2.xml', ''],
    'ВЕДОМОСТИ': ['https://www.vedomosti.ru/rss/news', '']
}


# parsing

def parse():
    data = {"data": []}

    for site in range(len(sites)):

        url = list(sites.values())[site][0]

        resp = req.get(url)

        soup = BeautifulSoup(resp.content, features="xml")

        tag_item = soup.find_all('item')

        for item in tag_item:
            data['data'].append({
                "title": item.title.text,
                "pubDate": item.pubDate.text,
                "site": list(sites.keys())[site],
            })

    return pd.DataFrame(data['data'])


df = parse()

# prediction

model = None

with (open('/mnt/nvme/Projects/DataEngineer/news_analysis/lenta.ru/models/knb_ppl_clf.pkl', 'rb')) as openfile:
    model = pickle.load(openfile)

predicted_topics = model.predict(df['title'])

# rename columns

df_news = pd.concat([pd.Series(predicted_topics), df], ignore_index=True, axis=1)

df_news = df_news.rename(columns={0: "category", 1: "title", 2: "pub_date", 3: "site"})

df_news['parsed_date'] = None
df_news['day_of_week'] = None
for index, row in enumerate(df_news['pub_date']):
    parsed = time.strptime(row, "%a, %d %b %Y %H:%M:%S %z")
    df_news['parsed_date'][index] = time.strftime('%Y-%m-%d %H:%M:%S', parsed)
    df_news['day_of_week'][index] = time.strftime('%a', parsed)

# kafka

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic = 'foobar'


def write_to_kafka(df):
    hm = {}
    for _, row in df.iterrows():
        hm["category"] = row["category"]
        hm["title"] = row["title"]
        hm["site"] = row["site"]
        hm["pub_date"] = row["parsed_date"]
        hm["day_of_week"] = row["day_of_week"]
        producer.send(topic, json.dumps(hm).encode('utf-8'))
        producer.flush()


# hbase

import happybase

connection = happybase.Connection('localhost')
table = connection.table('news')


def write_to_hbase(df):
    for i, row in df.iterrows():
        table.put(str.encode(str(i)), {
            b"cf:category": str.encode(row["category"]),
            b"cf:title": str.encode(row["title"]),
            b"cf:site": str.encode(row["site"]),
            b"cf:pub_date": str.encode(row["parsed_date"]),
            b"cf:day_of_week": str.encode(row["day_of_week"])
        })


# enums

from enum import IntEnum


class DataLoadingMode(IntEnum):
    Incremental = 1
    Initializing = 2


def start_writing(mode=DataLoadingMode.Initializing):
    if mode == DataLoadingMode.Initializing:
        print('Writing to HBase...')
        write_to_hbase(df_news)
    else:
        print('Writing to Kafka...')
        write_to_kafka(df_news)
    print('Done writing.')


if __name__ == '__main__':
    fire.Fire(start_writing)
