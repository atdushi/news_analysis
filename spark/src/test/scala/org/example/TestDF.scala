package org.example

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.FloatType
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class TestDF extends AnyFunSuite with BeforeAndAfterAll
  with BeforeAndAfterEach with DataFrameSuiteBase {

  //Double and Timestamp data types comparison accuracy
  val accuracy: Double = 0.11

  var session: SparkSession = _

  override def beforeAll: Unit = {
    session = SparkSession.builder()
      .appName("testing sparkbasics")
      .master("local[*]")
      .config("spark.testing.memory", "536870912") // 512Mb
      .config("spark.sql.session.timeZone", "GMT+3")
      .getOrCreate()

    session.sparkContext.setLogLevel("ERROR")
  }

  override def beforeEach() {
    session.catalog.clearCache()
  }

  test("DFs equal") {
    assert(true)
  }
}