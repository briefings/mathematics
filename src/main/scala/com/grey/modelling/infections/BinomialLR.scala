package com.grey.modelling.infections

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class BinomialLR(spark: SparkSession) {

  def binomialLR(dataEncoded: Dataset[Row], independentFactors: Array[String]): Unit = {

    val Array(training, testing) = dataEncoded.randomSplit(Array(0.8, 0.2), seed = 5)
    training.show(5)
    testing.show(5)

    val vectorAssembler: VectorAssembler = new VectorAssembler()
      .setInputCols(independentFactors)
      .setOutputCol("label")

    val trainingData: DataFrame = vectorAssembler.transform(training)
    val testingData: DataFrame = vectorAssembler.transform(testing)
    trainingData.show()
    testingData.show()

  }

}
