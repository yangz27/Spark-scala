package org.iscas.dataAwareJoin.join

import org.apache.spark.sql.{DataFrame, SparkSession}

object SortMergeJoin extends JoinStrategy {
  /**
    * 默认join算法，即SortMergeJoin算法
    * @param spark
    * @param dfLarge
    * @param dfMedium
    * @return
    */
  override def join(spark: SparkSession,
                    dfLarge: DataFrame,
                    dfMedium: DataFrame): DataFrame = {
    // 明确地禁止内部使用broadcastjoin
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

    dfLarge
      .join(
        dfMedium,
        Seq("key"),// join key
        "left_outer"
      )
      .select(
        dfLarge("key"),
        dfMedium("label")
      )
  }
}
