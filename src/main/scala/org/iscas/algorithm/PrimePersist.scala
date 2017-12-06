package org.iscas.algorithm

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.iscas.common.Consts

object PrimePersist {
  /**
    * 求素数,复用RDD
    * @param args
    */
  def main(args: Array[String]): Unit = {
    // https://www.iteblog.com/archives/1695.html
    val spark=SparkSession
      .builder()
      .master(Consts.MASTER)
      .appName("Prime")
      .getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("ERROR")

    val n=2000000
    val data=sc
      .parallelize(2 to n,8)
//      .persist(StorageLevel.MEMORY_ONLY)
    val composite=data
      .map(x=>(x,(2 to (n/x))))
      .flatMap(kv=>kv._2.map(_*kv._1))
    val prime=data.subtract(composite)
    println(prime.count())

  }
}
