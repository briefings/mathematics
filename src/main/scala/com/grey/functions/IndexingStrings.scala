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

    // Option I: Sequentially index each factor variable
    var indexer: StringIndexerModel = new StringIndexer().setInputCol(factors.head).setOutputCol(s"${factors.head}_index").fit(data)
    var indexed: DataFrame = indexer.transform(data)
    for (i <- 1 until factors.length) {
      indexer = new StringIndexer().setInputCol(factors(i)).setOutputCol(s"${factors(i)}_index").fit(indexed)
      indexed = indexer.transform(indexed)
    }

    // Export the Dataset[] form of the enhanced data
    val frame = ScalaCaseClass.scalaCaseClass(schema = indexed.schema)
    indexed.as(frame)

  }

}
