package com.grey.data

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType


class DataInterface(spark: SparkSession) {

  def dataInterface(dataString: String, schemaString: String): Dataset[Row] = {

    // Read-in the data schema
    val schema: StructType = new DataSchema(spark = spark).dataSchema(schemaString = schemaString)

    // Hence, read-in the corresponding data set
    var readings: DataFrame = new DataRead(spark = spark).dataRead(dataString = dataString, schema = schema)


    // Converting to a spark data set
    val frame: String = ScalaCaseClass.scalaCaseClass(schema = schema)
    readings.as(frame)

  }

}
