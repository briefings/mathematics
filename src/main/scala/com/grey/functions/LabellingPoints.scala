package com.grey.functions

import org.apache.spark.ml.feature.{LabeledPoint, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class LabellingPoints(spark: SparkSession) {

  def labellingPoints(data: Dataset[Row], independent: Array[String]): Unit = {

    /**
     * Import implicits for
     * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     * implicit conversions, e.g., converting a RDD to a DataFrames.
     * access to the "$" notation.
     */
    import spark.implicits._

    val points: RDD[LabeledPoint] = data.rdd.map(row =>
      new LabeledPoint(
        row.getAs[Double](fieldName = "label"),
        Vectors.dense(independent.map(h => row.getAs[Double](fieldName = h)))
      )
    )

    val instances: DataFrame = points.toDF()

    instances.show()



  }

}
