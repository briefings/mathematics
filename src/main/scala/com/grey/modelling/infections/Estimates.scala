package com.grey.modelling.infections


import com.grey.data.ScalaCaseClass
import com.grey.functions.{IndexingStrings, LabellingPoints}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 *
 * @param spark : A SparkSession instance
 */
class Estimates(spark: SparkSession) {

  private val independenceTest = new IndependenceTest(spark = spark)

  def estimates(infections: Dataset[Row]): Unit = {


    /**
     * Import implicits for
     * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     * implicit conversions, e.g., converting a RDD to a DataFrames.
     * access to the "$" notation.
     */


    // The outcome
    val label: String = "outcome"


    // Factor variables
    val factors = List("age", "sex", "asthma", "liver_mild", "renal", "pulmonary", "neurological",
      "liver_mod_severe", "malignant_neoplasm", label)


    // Extraneous variables
    val exclude = List("outcome_date", "admission_date")


    // Add extra features
    val extended: Dataset[Row] = new FeatureDuration(spark = spark).featureDuration(infections = infections)


    // Index each factor variable
    val indexed: Dataset[Row] = new IndexingStrings().indexingStrings(data = extended, factors = factors)


    // Hence, the modelling variables
    val arguments: DataFrame = indexed.drop(factors: _*).drop(exclude: _*)
      .withColumnRenamed(existingName = s"${label}_index", newName = "label")
    val variables: Dataset[Row] = arguments.as(
      ScalaCaseClass.scalaCaseClass(schema = arguments.schema))


    // The names of the independent variables
    val independent: Array[String] = variables.columns.filterNot(_ == s"${label}_index")
    independent.foreach(println(_))


    new LabellingPoints(spark = spark).labellingPoints(data = variables, independent = independent)

    // independenceTest.independenceTest(infections = infections)

  }

}
