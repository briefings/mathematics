package com.grey.modelling.infections

import com.grey.functions.{IndexingStrings, OneHotEncoding}
import org.apache.spark.sql.{Dataset, Row, SparkSession}


/**
 *
 * @param spark : A SparkSession instance
 */
class Estimates(spark: SparkSession) {

  private val independenceTest = new IndependenceTest(spark = spark)
  private val binomialLR = new BinomialLR(spark = spark)

  /**
   *
   * @param infections : The viral infections data set
   */
  def estimates(infections: Dataset[Row]): Unit = {


    println("\n\nInfections")


    /**
     * Import implicits for
     * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     * implicit conversions, e.g., converting a RDD to a DataFrames.
     * access to the "$" notation.
     *
     * import spark.implicits._
     *
     */


    // The dependent/outcome variable
    val label: String = "alive"


    // Factors
    val independentFactors: Array[String] = Array("age", "sex", "asthma", "liver_mild", "renal", "pulmonary", "neurological",
      "liver_mod_severe", "malignant_neoplasm")
    val factors: Array[String] = independentFactors ++ Array(label)


    // Extraneous variables
    val exclude: Array[String] = Array("outcome_date", "admission_date")
    exclude.foreach(println(_))


    // Add extra features
    val extended: Dataset[Row] = new FeatureDuration(spark = spark).featureDuration(infections = infections)
    extended.printSchema()


    // Index each factor variable
    val indexed: Dataset[Row] = new IndexingStrings().indexingStrings(data = extended, factors = factors)
    indexed.printSchema()
    indexed.selectExpr(factors.map(_ + "_index"):_*).show(numRows = 5)


    // Encoding
    val encoded: Dataset[Row] = new OneHotEncoding().oneHotEncoding(indexed = indexed, factors = factors)
    encoded.printSchema()
    encoded.selectExpr(factors.map(_ + "_enc"):_*).show(numRows = 5)


    // Independence
    independenceTest.independenceTest(dataIndexed = indexed,
      independentFactors = independentFactors.map(_ + "_index"), dependentFactor = s"${label}_index")

    // Regression
    binomialLR.binomialLR(dataEncoded = encoded,
      independentFactors = independentFactors.map(_ + "_index"), dependentFactor = s"${label}_index")


  }

}
