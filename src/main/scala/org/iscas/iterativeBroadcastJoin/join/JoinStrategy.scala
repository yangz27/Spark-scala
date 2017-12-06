package org.iscas.iterativeBroadcastJoin.join

import org.apache.spark.sql.{DataFrame, SparkSession}

trait JoinStrategy {
  /**
    * join算法
    * @param spark
    * @param dfLarge
    * @param dfMedium
    * @return
    */
  def join(spark:SparkSession,
           dfLarge:DataFrame,
           dfMedium:DataFrame):DataFrame
}
