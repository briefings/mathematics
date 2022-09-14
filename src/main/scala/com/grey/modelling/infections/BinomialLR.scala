package com.grey.modelling.infections

import com.grey.functions.LabellingPoints
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class BinomialLR(spark: SparkSession) {

  def binomialLR(dataEncoded: Dataset[Row], independentFactors: Array[String], dependentFactor: String): Unit = {

    // Prior to determining the degree of dependence between each <independent factor variable>
    // and the <dependent factor variable>, the data must be converted to a labelled point
    // structure.
    val dataLabelled: Dataset[Row] =  new LabellingPoints(spark = spark)
      .labellingPoints(data = dataEncoded, independent = independentFactors, dependent = dependentFactor)

    val Array(training, testing) = dataLabelled.randomSplit(Array(0.8, 0.2), seed = 5)

    training.show(5)
    testing.show(5)


  }

}
