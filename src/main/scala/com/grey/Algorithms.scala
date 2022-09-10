package com.grey

import com.grey.data.DataInterface
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.nio.file.Paths


/**
 *
 * @param spark : A SparkSession instance
 */
class Algorithms(spark: SparkSession) {

  private val dataInterface = new DataInterface(spark = spark)

  def algorithms(): Unit = {

    /**
     * Import implicits for
     * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     * implicit conversions, e.g., converting a RDD to a DataFrames.
     * access to the "$" notation.
     */


    // stock readings
    val stocks: Dataset[Row] = dataInterface.dataInterface(
      dataString = Paths.get("stocks", "apple.csv").toString,
      schemaString = Paths.get("stocks", "schema.json").toString)

    stocks.persist(StorageLevel.MEMORY_ONLY)

    new com.grey.modelling.stocks.Estimates(spark = spark)
      .estimates(stocks = stocks)


    // infections readings
    val infections: Dataset[Row] = dataInterface.dataInterface(
      dataString = Paths.get("infections", "viral.csv").toString,
      schemaString = Paths.get("infections", "schema.json").toString)

    infections.persist(StorageLevel.MEMORY_ONLY)

    new com.grey.modelling.infections.Estimates(spark = spark)
      .estimates(infections = infections)

  }

}
