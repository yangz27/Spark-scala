package org.iscas.operation

import org.apache.spark.sql.SparkSession
import org.iscas.common.Consts

object Cogroup {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("Cogroup")
      .master(Consts.MASTER)
      .getOrCreate()
    val sc=spark.sparkContext

    val data1=sc.parallelize(List((1,"www"),(2,"bbs"),(2,"bbs2")))
    val data2=sc.parallelize(List((1,"com"),(2,"iteblog"),(3,"very"),(3,"very2")))
    val data3=sc.parallelize(List((1,"com"),(2,"com"),(3,"good")))
    val result=data1.cogroup(data2,data3)
    result.collect().foreach(println)

//    (SliceJoin-2,(CompactBuffer(bbs, bbs2),CompactBuffer(iteblog),CompactBuffer(com)))
//    (SliceJoin-1,(CompactBuffer(www),CompactBuffer(com),CompactBuffer(com)))
//    (3,(CompactBuffer(),CompactBuffer(very, very2),CompactBuffer(good)))


  }
}
