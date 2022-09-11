package com.grey.functions

import com.grey.data.ScalaCaseClass
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/**
 *
 * @param spark: A SparkSession instance
 */
class LabellingPoints(spark: SparkSession) {


  /**
   *
   * @param data: A data set wherein the dependent/outcome variable is labelled/named <label>
   * @param independent: The names of the independent variables of <data>
   * @return
   */
  def labellingPoints(data: Dataset[Row], independent: Array[String]): Dataset[Row] = {

    /**
     * Import implicits for
     * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     * implicit conversions, e.g., converting a RDD to a DataFrames.
     * access to the "$" notation.
     */
    import spark.implicits._


    // Creating a labelled points RDD
    val points: RDD[LabeledPoint] = data.rdd.map(row =>
      new LabeledPoint(
        row.getAs[Double](fieldName = "label"),
        Vectors.dense(independent.map(h => row.getAs[Double](fieldName = h)))
      )
    )


    // Its data frame form
    val instances: DataFrame = points.toDF()


    // Its spark Dataset[] form
    instances.as(ScalaCaseClass.scalaCaseClass(schema = instances.schema))


  }

}
