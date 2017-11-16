package org.iscas.sql

import breeze.linalg.*
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession, functions, types}
import org.iscas.common.Consts

object BasicQuery {
  /**
    * 基本查询
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master(Consts.MASTER)
      .appName("BasicQuery")
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
    schemaRDD.createTempView("test")
    val q=spark.sql("select avg(sale) from test group by org")
    q.show()
    // 查看逻辑计划
    println(q.queryExecution)
//    == Parsed Logical Plan ==
//    'Aggregate ['org], [unresolvedalias('avg('sale), None)]
//    +- 'UnresolvedRelation `test`
//
//    == Analyzed Logical Plan ==
//    avg(sale): double
//    Aggregate [org#4], [avg(sale#5) AS avg(sale)#12]
//    +- SubqueryAlias test, `test`
//    +- LogicalRDD [id#3, org#4, sale#5]
//
//    == Optimized Logical Plan ==
//    Aggregate [org#4], [avg(sale#5) AS avg(sale)#12]
//    +- Project [org#4, sale#5]
//    +- LogicalRDD [id#3, org#4, sale#5]
//
//    == Physical Plan ==
//      *HashAggregate(keys=[org#4], functions=[avg(sale#5)], output=[avg(sale)#12])
//    +- Exchange hashpartitioning(org#4, 200)
//    +- *HashAggregate(keys=[org#4], functions=[partial_avg(sale#5)], output=[org#4, sum#17, count#18L])
//    +- *Project [org#4, sale#5]
//    +- Scan ExistingRDD[id#3,org#4,sale#5]


    // 查看物理计划
    q.explain

//    == Physical Plan ==
//      *HashAggregate(keys=[org#4], functions=[avg(sale#5)])
//    +- Exchange hashpartitioning(org#4, 200)
//    +- *HashAggregate(keys=[org#4], functions=[partial_avg(sale#5)])
//    +- *Project [org#4, sale#5]
//    +- Scan ExistingRDD[id#3,org#4,sale#5]

  }
}
