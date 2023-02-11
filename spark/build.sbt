ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.1",
  "org.apache.spark" %% "spark-sql" % "3.3.1",
  "org.apache.spark" %% "spark-streaming" % "3.3.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.1",
  "org.apache.hbase" % "hbase-server" % "2.5.2",
  "org.apache.hbase" % "hbase-mapreduce" % "2.5.2",
  "org.apache.hbase" % "hbase-client" % "2.5.2",
  "org.apache.hadoop" % "hadoop-common" % "3.2.4",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "3.3.1_1.4.0" % Test,

  "org.apache.hbase.connectors.spark" % "hbase-spark" % "1.0.1-SNAPSHOT"
    from "file:///mnt/nvme/Projects/DataEngineer/news_analysis/spark/lib/hbase-spark-1.0.1-SNAPSHOT.jar"
)

lazy val root = (project in file("."))
  .settings(
    name := "task1"
  )
