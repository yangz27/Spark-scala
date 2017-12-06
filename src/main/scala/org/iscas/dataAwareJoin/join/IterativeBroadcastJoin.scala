package org.iscas.dataAwareJoin.join

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.iscas.dataAwareJoin.common.{Config, SparkUtil}

import scala.annotation.tailrec

object IterativeBroadcastJoin extends JoinStrategy {

  /**
    *
    * @param spark
    * @param result 大表
    * @param broadcast 已经广播的中等规模表
    * @param iteration 第几次迭代
    * @return
    */
  @tailrec
  private def iterativeBroadcastJoin(spark:SparkSession,
                                     result:DataFrame,
                                     broadcast:DataFrame,
                                     iteration:Int=0):DataFrame=
    if( iteration<Config.numberOfBroadcastPasses ){// 只遍历numberOfBroadcastPasses次
      val tableName=s"tmp_broadcast_table_itr_$iteration.parquet"

      // 大表与小表join
      val out=result.join(
        // 得到该次遍历的所有key
        broadcast.filter(col("pass")===lit(iteration)),// lit函数产生一个列，值为iteration。
        Seq("key"),
        "left_outer"
      ).select(
        result("key"),
        coalesce(// 选择第一个非空的列
          result("label"),
          broadcast("label")
        ).as("label")// 确保新列名为label
      )
      // 持久化本次迭代的中间结果
      SparkUtil.dfWrite(out,tableName)

      iterativeBroadcastJoin(
        spark,
        SparkUtil.dfRead(spark,tableName),
        broadcast,
        iteration+1// 轮数+SliceJoin-1
      )

    }else result

  /**
    * 改进的join算法，iterative broadcast join
    * @param spark
    * @param dfLarge
    * @param dfMedium
    * @return
    */
  override def join(spark: SparkSession,
                    dfLarge: DataFrame,
                    dfMedium: DataFrame): DataFrame = {
    // 将中等规模表直接广播
    broadcast(dfMedium)
    // 调用算法
    iterativeBroadcastJoin(
      spark,
      dfLarge.select("key").withColumn("label",lit(null)),
      dfMedium
    )

  }
}
