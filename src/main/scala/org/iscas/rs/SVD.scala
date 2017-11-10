package org.iscas.rs

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.SparkSession
import org.iscas.common.Consts

object SVD {
  /**
    * SVD推荐
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master(Consts.MASTER)
      .appName("item based cf")
      .getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("ERROR")

    val data=sc.textFile(Consts.MASTER+"/user/lucio35/test.data")
    val parsedData=data.map(_.split(',') match {
      case Array(user,item,rate) => MatrixEntry(user.toLong-1,item.toLong-1,rate.toDouble)
    }).cache()
    val ratings=new CoordinateMatrix(parsedData)
    //           item
    //       ------------
    //       |
    //       |
    // user  |

    val svd=ratings.toRowMatrix().computeSVD(2,true)// 计算svd，返回奇异值个数为2
    // estimate user 1 -> item 1
    val score=(0 to 1).map(t => svd.U.rows.collect()(0)(t)*svd.V.transpose.toArray(t))// U是RowMatrix，V是Matrix
    println("rating of user 1 to item 1 is:"+score.sum*svd.s(0))
    // 如果不乘svd.s(0)，是如下的结果，可以看出，SVD做推荐如果没有填空缺值或者用0替代，效果很不好
    // rating of user 1 to item 1 is:0.5000000000000004
    // 乘svd.s(0)
    // rating of user 1 to item 1 is:6.000000000000004

  }
}
