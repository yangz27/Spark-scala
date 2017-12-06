package org.iscas.sql

import org.apache.spark.sql.SparkSession
import org.iscas.common.Consts

object ReduceByKeyCount {
  /**
    * 使用reduceByKey实现groupByKey然后计数的功能
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master(Consts.MASTER)
      .appName("ReduceByKeyCount")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc=spark.sparkContext

    val a = sc.parallelize(List((1,2),(1,3),(3,4),(3,6)))
    a.map(e => (e._1,1)).reduceByKey((x,y) => x+y).collect().foreach(println)

//    (SliceJoin-1,SliceJoin-2)
//    (3,SliceJoin-2)
  }
}
