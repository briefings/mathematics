package com.grey

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.grey.data.DataSchema
import com.grey.data.DataCaseClass.Stocks
import com.grey.data.DataRead
import org.apache.spark.sql.types.StructType

import java.nio.file.Paths

class Algorithms(spark: SparkSession) {

  def algorithms(): Unit = {

    /**
     * Import implicits for
     *    encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     *    implicit conversions, e.g., converting a RDD to a DataFrames.
     *    access to the "$" notation.
     */
    import spark.implicits._

    // Read-in the data schema
    val schema: StructType = new DataSchema(spark = spark)
      .dataSchema(dataString = Paths.get("stocks", "schema.json").toString)

    // Hence, read-in the corresponding data set
    val readings: DataFrame = new DataRead(spark = spark)
      .dataRead(dataString = Paths.get("stocks", "apple.csv").toString, schema = schema)

    // Convert the data frame to a spark data set; remember to import spark.implicits._
    val series = readings.as[Stocks]


  }

}
