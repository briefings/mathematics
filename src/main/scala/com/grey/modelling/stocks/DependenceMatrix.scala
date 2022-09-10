package com.grey.modelling.stocks

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{DataFrame, Dataset, Row}


/**
 *
 */
class DependenceMatrix() {

  /**
   *
   * @param stocks : The trading stocks data
   */
  def dependenceMatrix(stocks: Dataset[Row]): Unit = {

    // Hence
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("open", "close", "high", "low"))
      .setOutputCol("trade")

    // Collinearity
    val pearson: DataFrame = Correlation.corr(assembler.transform(stocks), column = "trade", method = "pearson")
    pearson.show()

    val spearman = Correlation.corr(assembler.transform(stocks), column = "trade", method = "spearman")
    spearman.show()


  }

}
