package com.grey.data

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType


class DataRead(spark: SparkSession) {

  def dataRead(dataString: String, schema: StructType): DataFrame = {

    // implicits
    import spark.implicits._

    // the data
    val readings: DataFrame = spark.read.schema(schema).format("csv")
      .option("header", value = true)
      .option("dateFormat", "yyyy-MM-dd")
      .option("encoding", "UTF-8")
      .load(dataString)

    readings

  }

}
