package com.grey.modelling.infections

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Estimates(spark: SparkSession) {

  def estimates(infections: Dataset[Row]): Unit = {

    /**
     * Import implicits for
     *    encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     *    implicit conversions, e.g., converting a RDD to a DataFrames.
     *    access to the "$" notation.
     */
    import spark.implicits._

    infections.select($"admission_date", $"age", $"asthma").show()

  }

}
