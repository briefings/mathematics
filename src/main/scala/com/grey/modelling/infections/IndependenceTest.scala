package com.grey.modelling.infections

import com.grey.functions.LabellingPoints
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/**
 *
 * @param spark: A SparkSession instance
 */
class IndependenceTest(spark: SparkSession) {

  /**
   *
   * @param dataIndexed: A data set wherein the factor variables have been indexed
   * @param independentFactors: The array of independent factors
   */
  def independenceTest(dataIndexed: Dataset[Row], independentFactors: Array[String], dependentFactor: String): Unit = {

    /**
     * Import spark.implicits._ for
     * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     * implicit conversions, e.g., converting a RDD to a DataFrames.
     * access to the "$" notation.
     */


    // Prior to determining the degree of dependence between each <independent factor variable>
    // and the <dependent factor variable>, the data must be converted to a labelled point
    // structure.
    val dataLabelled: Dataset[Row] =  new LabellingPoints(spark = spark)
      .labellingPoints(data = dataIndexed, independent = independentFactors, dependent = dependentFactor)


    // Hence
    val chi: DataFrame = ChiSquareTest.test(dataLabelled, featuresCol = "features", labelCol = "label")
    chi.printSchema()


    // The estimates
    println(independentFactors.mkString(", "))
    println("probabilities: " + chi.head.getAs[Vector[Double]](fieldName = "pValues"))
    println("degrees of freedom: " +  chi.head.getAs[Vector[IntegerType]](fieldName = "degreesOfFreedom"))
    println("statistics: " + chi.head.getAs[Vector[Double]](fieldName = "statistics"))


  }

}
