package com.grey.modelling.stocks

import org.apache.spark.sql.{Dataset, Row, SparkSession}


/**
 *
 * @param spark : A spark instance
 */
class Estimates(spark: SparkSession) {

  private val dependenceMatrix = new DependenceMatrix()

  /**
   *
   * @param stocks : The trading stocks data
   */
  def estimates(stocks: Dataset[Row]): Unit = {

    /**
     * Import implicits for
     * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     * implicit conversions, e.g., converting a RDD to a DataFrames.
     * access to the "$" notation.
     */
    import spark.implicits._

    // A preview of stocks
    stocks.select(cols = $"open", $"close", $"high", $"low").show()

    // The degree of correlation between the 4 stock level variables
    dependenceMatrix.dependenceMatrix(stocks = stocks)

  }

}
