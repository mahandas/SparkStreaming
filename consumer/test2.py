# Databricks notebook source
# MAGIC %sh pip install regex
# MAGIC pip install textblob
# MAGIC pip install elasticsearch

# COMMAND ----------

from kafka import KafkaConsumer
from json import loads
import json
from textblob import TextBlob
import re
from elasticsearch import Elasticsearch
from pyspark.sql import SQLContext

consumer = KafkaConsumer(
    'numtest',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

def getHashTag(text):
    if "trump" in text.lower():
        return "#trump"
    else:
        return "#corona"

def clean_tweet(tweet): 
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split()) 


def get_tweet_sentiment(tweet): 
    ''' 
    Utility function to classify sentiment of passed tweet 
    using textblob's sentiment method 
    '''
    # create TextBlob object of passed tweet text 
    analysis = TextBlob(clean_tweet(tweet)) 
    # set sentiment 
    if analysis.sentiment.polarity > 0: 
        return 'positive'
    elif analysis.sentiment.polarity == 0: 
        return 'neutral'
    else: 
        return 'negative'
      
def getSentiment(rdd):
  test = rdd.collect()
  print(test[0])
  d = {"sentiment": test[0][0], "hashtag": test[0][1], "tweet":test[0][2]}
  esConn.index(index="hash_tags_sentiment_analysis", doc_type="tweet-sentiment-analysis", body=d)
  

esConn = Elasticsearch([{'host': '18.189.188.39', 'port': 9200}])
for message in consumer:
    #message = [getHashTag(message.value),get_tweet_sentiment(message.value), message.value]
    df = sc.parallelize([message.value])
    rdd = df.map(lambda x: (get_tweet_sentiment(x), getHashTag(message.value), x))
    #new_rdd = rdd.map(json.dumps).map(lambda x: ('key', x))
    getSentiment(rdd)

# COMMAND ----------



