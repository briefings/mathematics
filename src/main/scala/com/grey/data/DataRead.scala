package com.grey.data

import com.grey.environment.LocalSettings
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

import java.nio.file.Paths


class DataRead(spark: SparkSession) {

  private val localSettings = new LocalSettings()

  def dataRead(dataString: String, schema: StructType): DataFrame = {

    // the data
    val readings: DataFrame = spark.read.schema(schema).format("csv")
      .option("header", value = true)
      .option("dateFormat", "yyyy-MM-dd")
      .option("encoding", "UTF-8")
      .load(Paths.get(localSettings.dataDirectory, dataString).toString)

    readings

  }

}
