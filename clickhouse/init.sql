DROP DATABASE IF EXISTS de;

CREATE DATABASE IF NOT EXISTS de;

-- HDFS

CREATE OR REPLACE TABLE de.news (
    pub_date Datetime,
    day_of_week String,
    site String,
    category String,
    title String)
ENGINE=HDFS('hdfs://localhost:9000/news/*', 'JSONEachRow');

-- Kafka

CREATE TABLE de.news_topic (
	pub_date Datetime,
    day_of_week String,
    site String,
    category String,
    title String
) ENGINE=Kafka('localhost:9092', 'foobar', 'group1', 'JSONEachRow');

CREATE TABLE de.news_target (
	pub_date Datetime,
    day_of_week String,
    site String,
    category String,
    title String
) ENGINE=MergeTree
PARTITION BY toYYYYMM(pub_date)
ORDER BY tuple();

CREATE MATERIALIZED VIEW de.vw_news TO news_target
AS SELECT pub_date, day_of_week, site, category, title
FROM de.news_topic;