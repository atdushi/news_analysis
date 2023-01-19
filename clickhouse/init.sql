DROP DATABASE IF EXISTS de;

CREATE DATABASE IF NOT EXISTS de;

CREATE OR REPLACE TABLE de.news (
    pub_date Datetime,
    site String,
    category String,
    title String) 
ENGINE=HDFS('hdfs://localhost:9000/news/*', 'JSONEachRow');
