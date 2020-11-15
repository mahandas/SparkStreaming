# SparkStreaming
Streaming tweets using Kafka servers on spark cluster of databricks, analyzing the tweets for basic sentiment classification and visualization of data using KIbana in elastic search that is set up in AWS EC2 instance.

# Tech Stack

![alt text](https://github.com/mahandas/SparkStreaming/blob/main/static/Screen%20Shot%202020-11-14%20at%208.05.08%20PM.png?raw=true)

# Architecture 

![alt text](https://github.com/mahandas/SparkStreaming/blob/main/static/Screen%20Shot%202020-11-14%20at%208.22.44%20PM.png?raw=true)

Everything is on a combination of databricks and aws ec2 instance. Now lets deep dive into the details.

## 1. Kafka Setup
- Databricks platform is used to setup kafka in spark clusters
- Both zookeeper instance and kafka instances are ran on two different notebooks
- A notebook is used for the producer where the Scrapper is run.
- Another notebook is used for consumer where sentiment analysis is done on the tweets.

## 2. Scrapper 

- The scrapper will collect all tweets using tweepy api and sends them to Kafka for analytics.
- Collecting tweets in real-time with particular hash tags. For example, we
will collect all tweets with #trump, #coronavirus.
- After filtering, we will send them to Kafka.
- the scrapper program runs infinitely and takes hash tag as input parameter while running.

## 3. Spark Streaming
- In Spark Streaming, we need to create a Kafka consumer and periodically collect filtered tweets from scrapper.
- This is done in databricks clusters.
- For each hash tag, we perform basic sentiment analysis to classify the tweets as - neutral, negative and positive.

## 4. Sentiment Analyzer
- Sentiment Analysis is the process of determining whether a piece of writing is positive, negative or neutral. It's also known as opinion mining, deriving the opinion or attitude of a speaker.
- For example,

1. “President Donald Trump approaches his first big test this week from a
position of unusual weakness.” - has positive sentiment.

2. “Trump has the lowest standing in public opinion of any new president in
modern history.” - has neutral sentiment.

3. “Trump has displayed little interest in the policy itself, casting it as a
thankless chore to be done before getting to tax-cut legislation he values
more.” - has negative sentiment.

## 5. Elasticsearch
- We need to install the Elasticsearch and run it to store the tweets and their sentiment information for further visualization purpose.
- Elasticsearch is installed in AWS ec2 instance and accesed publically for saving the sentiment information.

## 6. Kibana
- Kibana is a visualization tool that can explore the data stored in Elasticsearch. 
- Kibana is installed as in aws ec2 intance.
- Here, instead of directly output the result, we use the visualization tool to show tweets sentiment classification result in a real-time manner. 

