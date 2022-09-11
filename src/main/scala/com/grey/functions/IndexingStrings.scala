package com.grey.functions

import com.grey.data.ScalaCaseClass
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row}


/**
 *
 */
class IndexingStrings {

  /**
   *
   * @param data: A data set
   * @param factors: The list of factor variables whose field elements will be indexed.
   * @return
   */
  def indexingStrings(data: Dataset[Row], factors: Array[String]): Dataset[Row] = {

    val indexer: StringIndexer = new StringIndexer().setInputCols(factors).setOutputCols(factors.map(_ + "_index"))
    val indexerModel: StringIndexerModel = indexer.fit(data)
    val indexed: DataFrame = indexerModel.transform(data)

    

    // Export the Dataset[] form of the enhanced data
    val frame = ScalaCaseClass.scalaCaseClass(schema = indexed.schema)
    indexed.as(frame)

  }

}
