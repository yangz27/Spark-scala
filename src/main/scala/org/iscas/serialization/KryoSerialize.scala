package org.iscas.serialization

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.iscas.common.Consts
import org.jpmml.schema.Added

object KryoSerialize {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster(Consts.MASTER)
      .setAppName("KryoSerialize")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[X]))
    val sc=new SparkContext(conf)
//    sc.setLogLevel("ERROR")

    val data=sc.textFile(Consts.HDFS+"/input/test_big.csv")
    val dataX=data.map(e => {
      val s=e.split(",")
      val ee=new X(s(0),s(1),s(2))
      ee
    })
    dataX.persist(StorageLevel.MEMORY_ONLY_SER)
    println(dataX.count())

//    17/11/20 11:36:42 INFO storage.BlockManagerInfo: Added rdd_2_0 in memory on 133.133.30.10:37149 (size: 34.7 MB, free: 331.5 MB)
//    17/11/20 11:36:42 INFO storage.BlockManagerInfo: Added rdd_2_1 in memory on 133.133.30.10:37149 (size: 34.7 MB, free: 296.8 MB)

  }
}
