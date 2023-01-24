package org.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions.{col, get_json_object}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.conf.Configuration

object App {
  private object DataLoadingMode extends Enumeration {
    val Test = Value
    val Incremental = Value
    val Initializing = Value
  }

  private val DATA_LOADING_MODE = DataLoadingMode.Initializing

  private val parse = Map[DataLoadingMode.Value, () => Unit](
    DataLoadingMode.Test -> streamTest,
    DataLoadingMode.Incremental -> parseKafka,
    DataLoadingMode.Initializing -> parseHBase)

  def main(args: Array[String]): Unit = {
    //    parseKafka()
    //    parseHBase()

    parse(DATA_LOADING_MODE)()
  }

  private def parseHBase(): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val conf = new Configuration()
    //    conf.set("hbase.zookeeper.quorum", "127.0.0.1:10231")

    new HBaseContext(spark.sparkContext, conf)

    val df = spark.read.format("org.apache.hadoop.hbase.spark")
      .option("hbase.columns.mapping",
        """rowKey STRING :key,
          |category STRING cf:category,
          |title STRING cf:title,
          |site STRING cf:site,
          |pub_date STRING cf:pub_date,
          |day_of_week STRING cf:day_of_week""".stripMargin
      )
      .option("hbase.spark.pushdown.columnfilter", false)
      .option("hbase.table", "news")
      .load()

    df.show()

    df.write.mode("append").json("hdfs://localhost:9000/news")
  }

  private def parseKafka(): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val ds1 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "foobar")
      .option("startingOffsets", "earliest")
      .option("auto.offset.reset", "earliest")
      .load()

    ds1
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset")
      .writeStream
      .trigger(ProcessingTime("10 seconds")) // save data every 10 seconds
      .foreachBatch((batchDF: DataFrame, batchId: Long) => {
        batchDF.persist()

        //batchDF.write.format("console").mode("append").save()

        val df = batchDF
          .select(
            get_json_object(col("value"), "$.pub_date").alias("pub_date"),
            get_json_object(col("value"), "$.day_of_week").alias("day_of_week"),
            get_json_object(col("value"), "$.site").alias("site"),
            get_json_object(col("value"), "$.category").alias("category"),
            get_json_object(col("value"), "$.title").alias("title"))

        df.write
          .format("console")
          .mode("append")
          .save()

        df.write
          .format("json")
          .mode("append")
          .option("path", "hdfs://localhost:9000/news")
          .option("checkpointLocation", "/tmp/task1/checkpoint")
          .save()

        val u = batchDF.unpersist()
      })
      .start()
      .awaitTermination(10000);
  }

  private def streamTest(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination

    // client:
    // nc -lk 9999
  }

}
