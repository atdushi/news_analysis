DROP DATABASE IF EXISTS de;

CREATE DATABASE IF NOT EXISTS de;

CREATE OR REPLACE TABLE de.news (
    pub_date Datetime,
    day_of_week String,
    site String,
    category String,
    title String)
ENGINE=HDFS('hdfs://localhost:9000/news/*', 'JSONEachRow');
