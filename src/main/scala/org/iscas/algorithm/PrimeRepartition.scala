package org.iscas.algorithm

import org.apache.spark.sql.SparkSession
import org.iscas.common.Consts
object PrimeRepartition {
  /**
    * 求素数
    * 改进：repatition
    * @param args
    */
  def main(args: Array[String]): Unit = {
    // https://www.iteblog.com/archives/1695.html
    val spark=SparkSession
      .builder()
      .master(Consts.MASTER)
      .appName("PrimeRepartition")
      .getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("ERROR")

    val n=2000000
    val composite=sc
      .parallelize(2 to n,8)
      .map(x=>(x,(2 to (n/x))))
      .repartition(8)
      .flatMap(kv=>kv._2.map(_*kv._1))
    val prime=sc.parallelize(2 to n,8).subtract(composite)
    println(prime.count())

  }
}
