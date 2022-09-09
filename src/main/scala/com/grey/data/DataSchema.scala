package com.grey.data

import com.grey.environment.LocalSettings
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}

import java.nio.file.Paths
import scala.util.Try
import scala.util.control.Exception

/**
 *
 * @param spark: A SparkSession instance
 */
class DataSchema(spark: SparkSession) {

  private val localSettings = new LocalSettings()

  /**
   *
   * @param dataString: A data file's path & file string w.r.t. this project's <data> directory
   */
  def dataSchema(dataString: String): Try[StructType] = {

    // reading-in a data schema
    val fieldProperties: Try[RDD[String]] = Exception.allCatch.withTry(
      spark.sparkContext.textFile(Paths.get(localSettings.dataDirectory, dataString).toString)
    )

    // convert the schema to a StructType
    val schema: Try[StructType] = if (fieldProperties.isSuccess) {
      Exception.allCatch.withTry(
        DataType.fromJson(fieldProperties.get.collect.mkString("")).asInstanceOf[StructType]
      )
    } else {
      sys.error(fieldProperties.failed.get.getMessage)
    }

    schema

  }

}
