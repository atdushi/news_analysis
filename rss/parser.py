import requests as req 
from bs4 import BeautifulSoup, CData, Tag
import time
import tqdm
import pickle
import pandas as pd

sites = { 
    'Фонтанка.ру' : ['https://www.fontanka.ru/fontanka.rss', ''],                  
    'LENTA.RU' : ['https://lenta.ru/rss', ''],                  
    'TASS' : ['https://tass.ru/rss/v2.xml', ''],
    'ВЕДОМОСТИ' : ['https://www.vedomosti.ru/rss/news', '']           
}

# parsing

def parse():
    data = { "data" : [] }
        
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

with (open('/mnt/nvme/Projects/DataEngineer/news_analysis/lenta/models/knb_ppl_clf.pkl', 'rb')) as openfile:
    model = pickle.load(openfile)

predicted_topics = model.predict(df['title'])

# rename columns

df_news = pd.concat([pd.Series(predicted_topics), df], ignore_index=True, axis=1)

df_news = df_news.rename(columns={0: "category", 1: "title", 2: "pub_date", 3: "site"})

df_news['date'] = None
for index, row in enumerate(df_news['pub_date']):
    parsed = time.strptime(row, "%a, %d %b %Y %H:%M:%S %z")
    df_news['date'][index] = time.strftime('%d/%m/%Y', parsed)

# kafka

import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def write_to_kafka(df):
    hm = {}
    for _, row in df.iterrows():
        hm["category"] = row["category"]
        hm["title"] = row["title"]
        hm["site"] = row["site"]
        hm["pub_date"] = row["pub_date"]        
        producer.send('foobar', json.dumps(hm).encode('utf-8'))
        producer.flush()

write_to_kafka(df_news)