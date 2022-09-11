package com.grey.modelling.infections

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.datediff

import com.grey.data.ScalaCaseClass

class FeatureDuration(spark: SparkSession) {

  def featureDuration(infections: Dataset[Row]): Dataset[Row] = {

    /**
     * Import implicits for
     * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     * implicit conversions, e.g., converting a RDD to a DataFrames.
     * access to the "$" notation.
     */
    import spark.implicits._


    // Feature: Length of stay
    var extended: DataFrame = infections
      .withColumn(colName = "duration_days", col = datediff(end = $"outcome_date", start = $"admission_date"))
    println(extended.count())


    // Error: Exclude observations wherein (<length of stay> < 0 | <length of stay> is empty)
    extended = extended.filter($"duration_days" >= 0 && $"duration_days".isNotNull)
    println(extended.count())


    // Export the Dataset[] form
    val frame = ScalaCaseClass.scalaCaseClass(schema = extended.schema)
    extended.as(frame)


  }

}
