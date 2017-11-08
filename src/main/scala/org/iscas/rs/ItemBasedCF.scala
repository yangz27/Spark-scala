package org.iscas.rs

import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.sql.SparkSession
import org.iscas.common.Consts

object ItemBasedCF {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master(Consts.MASTER)
      .appName("item based cf")
      .getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("ERROR")

    val data=sc.textFile("hdfs://master:9000/user/lucio35/test.data")
    val parsedData=data.map(_.split(',') match {
      case Array(user,item,rate) => MatrixEntry(user.toLong-1,item.toLong-1,rate.toDouble)
    }).cache()
    val ratings=new CoordinateMatrix(parsedData)
    //           item
    //       ------------
    //       |
    //       |
    // user  |
    val similarities=ratings.toRowMatrix().columnSimilarities(0)// 精确的物品相似度

    val ratingsOfItem1=ratings.transpose().toRowMatrix().rows.collect()(0).toArray
    val avgRatingToItem1=ratingsOfItem1.sum/ratingsOfItem1.size// 物品1的平均得分



  }
}
