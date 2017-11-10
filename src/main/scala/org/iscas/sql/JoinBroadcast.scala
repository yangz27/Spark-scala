package org.iscas.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.iscas.common.Consts
import org.apache.spark.sql.functions.broadcast

object JoinBroadcast {
  /**
    * join
    * 使用了functions.broadcast将dataframe压缩，以便用到BroadcastHashJoin中
    * @param args
    */
  def main(args: Array[String]): Unit = {
        val spark=SparkSession
      .builder()
      .master(Consts.MASTER)
      .appName("basic-query")
      .getOrCreate()
    val sc=spark.sparkContext
    val sqlC=spark.sqlContext
    sc.setLogLevel("ERROR")

    var test=sc.textFile(Consts.HDFS+"/input/test.csv")
    test=test.filter(! _.contains("id"))
    val rowRDD=test.map(_.split(",")).map(line => Row(line(0).toInt,line(1),line(2).toDouble))
    val schema=StructType(
      List(
        StructField("id",IntegerType,true),
        StructField("org",StringType,true),
        StructField("sale",DoubleType,true)
      )
    )
    val schemaRDD=sqlC.createDataFrame(rowRDD,schema)
//    schemaRDD.createTempView("test")

    val joinDF=schemaRDD.join(broadcast(schemaRDD),Seq("id"))
    joinDF.show()

    println(joinDF.queryExecution)
//    == Parsed Logical Plan ==
//    'Join UsingJoin(Inner,List(id))
//    :- LogicalRDD [id#3, org#4, sale#5]
//    +- BroadcastHint
//      +- LogicalRDD [id#11, org#12, sale#13]
//
//    == Analyzed Logical Plan ==
//    id: int, org: string, sale: double, org: string, sale: double
//    Project [id#3, org#4, sale#5, org#12, sale#13]
//    +- Join Inner, (id#3 = id#11)
//    :- LogicalRDD [id#3, org#4, sale#5]
//    +- BroadcastHint
//      +- LogicalRDD [id#11, org#12, sale#13]
//
//    == Optimized Logical Plan ==
//    Project [id#3, org#4, sale#5, org#12, sale#13]
//    +- Join Inner, (id#3 = id#11)
//    :- Filter isnotnull(id#3)
//    :  +- LogicalRDD [id#3, org#4, sale#5]
//    +- BroadcastHint
//      +- Filter isnotnull(id#11)
//    +- LogicalRDD [id#11, org#12, sale#13]
//
//    == Physical Plan ==
//      *Project [id#3, org#4, sale#5, org#12, sale#13]
//    +- *BroadcastHashJoin [id#3], [id#11], Inner, BuildRight
//    :- *Filter isnotnull(id#3)
//    :  +- Scan ExistingRDD[id#3,org#4,sale#5]
//    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)))
//    +- *Filter isnotnull(id#11)
//    +- Scan ExistingRDD[id#11,org#12,sale#13]
  }
}
