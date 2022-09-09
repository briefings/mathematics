package com.grey

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MathematicsApp {

  def main(args: Array[String]): Unit = {

    // Limiting log streams
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // A SparkSession instance
    val spark: SparkSession = SparkSession.builder().appName("mathematics")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // Spark logs
    spark.sparkContext.setLogLevel("ERROR")


  }

}
