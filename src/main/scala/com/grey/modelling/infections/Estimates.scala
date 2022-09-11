package com.grey.modelling.infections


import com.grey.data.ScalaCaseClass
import com.grey.functions.{IndexingStrings, OneHotEncoding}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 *
 * @param spark : A SparkSession instance
 */
class Estimates(spark: SparkSession) {

  private val independenceTest = new IndependenceTest(spark = spark)

  def estimates(infections: Dataset[Row]): Unit = {


    println("\n\nInfections")


    /**
     * Import implicits for
     * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     * implicit conversions, e.g., converting a RDD to a DataFrames.
     * access to the "$" notation.
     */


    // The dependent/outcome variable
    val label: String = "outcome"


    // Factors
    val independentFactors: Array[String] = Array("age", "sex", "asthma", "liver_mild", "renal", "pulmonary", "neurological",
      "liver_mod_severe", "malignant_neoplasm")
    val factors: Array[String] = independentFactors ++ Array(label)


    // Extraneous variables
    val exclude: Array[String] = Array("outcome_date", "admission_date")


    // Add extra features
    val extended: Dataset[Row] = new FeatureDuration(spark = spark).featureDuration(infections = infections)
    extended.printSchema()


    // Index each factor variable
    val indexed: Dataset[Row] = new IndexingStrings().indexingStrings(data = extended, factors = factors)
    indexed.printSchema()


    // Encoding
    val encoded: Dataset[Row] = new OneHotEncoding().oneHotEncoding(indexed = indexed, factors = factors)
    encoded.printSchema()


    // Hence, the modelling variables
    val arguments: DataFrame = indexed.drop(factors: _*).drop(exclude: _*)
    val dataIndexed: Dataset[Row] = arguments.as(
      ScalaCaseClass.scalaCaseClass(schema = arguments.schema))
    dataIndexed.printSchema()


    // Independence
    independenceTest.independenceTest(dataIndexed = dataIndexed,
      independentFactors = independentFactors.map(_ + "_index"), dependentFactor = s"${label}_index")

  }

}
