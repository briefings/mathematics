package com.grey.modelling.infections

import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class IndependenceTest(spark: SparkSession) {

  def independenceTest(data: Dataset[Row]): Unit = {

    /**
     * Import spark.implicits._ for
     * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     * implicit conversions, e.g., converting a RDD to a DataFrames.
     * access to the "$" notation.
     */

    val chi: DataFrame = ChiSquareTest.test(data, featuresCol = "features", labelCol = "label")

    chi.show()


  }

}
