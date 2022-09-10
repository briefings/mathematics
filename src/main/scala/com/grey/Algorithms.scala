package com.grey

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import com.grey.data.DataInterface
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.storage.StorageLevel

import java.nio.file.Paths

class Algorithms(spark: SparkSession) {

  def algorithms(): Unit = {

    /**
     * Import implicits for
     *    encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
     *    implicit conversions, e.g., converting a RDD to a DataFrames.
     *    access to the "$" notation.
     */
    import spark.implicits._

    // stock readings
    val stocks: Dataset[Row] = new DataInterface(spark = spark).dataInterface(
      dataString = Paths.get("stocks", "apple.csv").toString,
      schemaString = Paths.get("stocks", "schema.json").toString)

    // Persistence
    stocks.persist(StorageLevel.MEMORY_ONLY)

    // Hence
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("open", "close"))
      .setOutputCol("trade")

    // Collinearity
    val pearson: DataFrame = Correlation.corr(assembler.transform(stocks), column = "trade", method = "pearson")
    pearson.show()

    val spearman = Correlation.corr(assembler.transform(stocks), column = "trade", method = "spearman")
    spearman.show()

    stocks.select(cols = $"open", $"close").show()

  }

}
