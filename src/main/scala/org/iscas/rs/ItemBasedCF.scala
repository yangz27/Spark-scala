package org.iscas.rs

import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.sql.SparkSession
import org.iscas.common.Consts

object ItemBasedCF {
  /**
    * item-based协同过滤
    * 数据来自$SPARK_HOME/data/mllib/als/test.data
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master(Consts.MASTER)
      .appName("ItemBasedCF")
      .getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("ERROR")

    val data=sc.textFile(Consts.HDFS+"/user/lucio35/test.data")
    val parsedData=data.map(_.split(',') match {
      case Array(user,item,rate) => MatrixEntry(user.toLong-1,item.toLong-1,rate.toDouble)
    }).cache()
    val ratings=new CoordinateMatrix(parsedData)
    //           item
    //       ------------
    //       |
    //       |
    // user  |
    val similarities=ratings.toRowMatrix().columnSimilarities(0)// 精确的物品相似度，是cosine sim

//    val ratingsOfItem1=ratings.transpose().toRowMatrix().rows.collect()(0).toArray
//    val avgRatingToItem1=ratingsOfItem1.sum/ratingsOfItem1.size// 物品1的平均得分
    // estimate user 1->item 1

    val ratingsOfUser1=ratings.toRowMatrix().rows.collect()(0).toArray.drop(1)// 用户1对其他物品的打分
    val columnsMean=ratings.toRowMatrix().computeColumnSummaryStatistics().mean
    val avgRatingOfItem1=columnsMean.toArray(0)
    val avgRatingOfItem=columnsMean.toArray.drop(1)// 其他物品的平均得分
    println(avgRatingOfItem1)// 3
    avgRatingOfItem.foreach(println)// [3,3,3]

    val weights=similarities.entries.filter(_.i==0).sortBy(_.j).map(_.value).collect()// 物品1和其他物品的相似度
    val weightedR=(0 to 2).map(t => weights(t)*(ratingsOfUser1(t)-avgRatingOfItem(t))).sum/weights.sum
    weights.foreach(println)// [0.38,1,0.38]

    println("rating of user 1 to item 1 is:"+(avgRatingOfItem1+weightedR))
    // rating of user 1 to item 1 is:3.2608695652173916
  }
}
