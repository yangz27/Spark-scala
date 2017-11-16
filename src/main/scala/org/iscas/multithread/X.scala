package org.iscas.multithread

import org.apache.spark.sql.SparkSession
import org.iscas.common.Consts

class X(_tableName:String,_spark:SparkSession) extends Thread{
  val tableName=_tableName
  val spark=_spark

  override
  def run(): Unit = {
    println("do query "+tableName)
    val table=spark.read.parquet(Consts.HDFS+"/opt/parquetStore/health/"+_tableName)
    table.createTempView(tableName)
    spark.sql("select count(*) from "+tableName).show()
    spark.sql("select * from "+tableName+" limit 5").show()
    spark.sql("select count(*) from "+tableName).show()

  }
}
