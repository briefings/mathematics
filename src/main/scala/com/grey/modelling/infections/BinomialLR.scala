package com.grey.modelling.infections

import com.grey.functions.LabellingPoints
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class BinomialLR(spark: SparkSession) {

  def binomialLR(dataEncoded: Dataset[Row], independentFactors: Array[String], dependentFactor: String): Unit = {

    val Array(training, testing) = dataEncoded.randomSplit(Array(0.8, 0.2), seed = 5)
    training.show(5)
    testing.show(5)

    

  }

}
