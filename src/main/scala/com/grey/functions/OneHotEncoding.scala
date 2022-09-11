package com.grey.functions

import com.grey.data.ScalaCaseClass
import org.apache.spark.ml.feature.{OneHotEncoder, OneHotEncoderModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.language.postfixOps


/**
 *
 */
class OneHotEncoding() {

  /**
   *
   * @param indexed: A data set wherein the factor variables have been indexed
   * @param factors: The names of the factor variables, excluding their suffix <_index>.
   * @return
   */
  def oneHotEncoding(indexed: Dataset[Row], factors: Array[String]): Dataset[Row] = {

    /**
     * Import implicits for
     * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     * implicit conversions, e.g., converting a RDD to a DataFrames.
     * access to the "$" notation.
     *
     * import spark.implicits._
     *
     */


    // Each indexed factor variable has suffix <_index>
    // Each encoded factor variable will have suffix <_enc>
    val indices: Array[String] = factors.map(_ + "_index")
    val encodings: Array[String] = factors.map(_ + "_enc")


    // The encoder
    val encoder: OneHotEncoder = new OneHotEncoder().setInputCols(indices).setOutputCols(encodings)


    // ... its model
    val encoderModel: OneHotEncoderModel = encoder.fit(indexed)


    // ... hence, the data transformation
    val encoded: DataFrame = encoderModel.transform(indexed)


    // Export the spark Dataset[] form
    encoded.as(ScalaCaseClass.scalaCaseClass(schema = encoded.schema))


  }

}
