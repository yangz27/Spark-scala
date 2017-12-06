package org.iscas.serialization

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.iscas.common.Consts
import org.jpmml.schema.Added

object JavaSerialize {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master(Consts.MASTER)
      .appName("JavaSerialize")
      .getOrCreate()
    val sc=spark.sparkContext
//    sc.setLogLevel("ERROR")

    val data=sc.textFile(Consts.HDFS+"/input/test_big.csv")
    val dataX=data.map(e => {
      val s=e.split(",")
      val ee=new X(s(0),s(1),s(2))
      ee
    })
    dataX.persist(StorageLevel.MEMORY_ONLY_SER)
    println(dataX.count())

//    17/11/20 11:11:48 INFO storage.BlockManagerInfo: Added rdd_2_0 in memory on 133.133.30.10:33169 (size: 61.9 MB, free: 304.3 MB)
//    17/11/20 11:11:48 INFO storage.BlockManagerInfo: Added rdd_2_1 in memory on 133.133.30.10:33169 (size: 61.9 MB, free: 242.4 MB)


  }
}
