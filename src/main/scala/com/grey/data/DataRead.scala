package com.grey.data

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import com.grey.data.DataCaseClass.Stocks

class DataRead(spark: SparkSession) {

  def dataRead(dataString: String, schema: StructType): Dataset[Stocks] = {

    // implicits
    import spark.implicits._

    // the data
    val readings: DataFrame = spark.read.schema(schema).format("csv")
      .option("header", value = true)
      .option("dateFormat", "yyyy-MM-dd")
      .option("encoding", "UTF-8")
      .load(dataString)

    // as a dataset
    readings.as[Stocks]

  }

}
