package org.iscas.sql

import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.iscas.common.Consts

object Join {
  /**
    * 基本的join
    * 可以看出Spark默认是SortMergeJoin
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master(Consts.MASTER)
      .appName("Join")
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

    val q=spark.sql("select * from test t1,test t2 where t1.id=t2.id")
    q.show()

    println(q.queryExecution)
//    == Parsed Logical Plan ==
//    'Project [*]
//    +- 'Filter ('t1.id = 't2.id)
//    +- 'Join Inner
//      :- 'UnresolvedRelation `test`, t1
//    +- 'UnresolvedRelation `test`, t2
//
//    == Analyzed Logical Plan ==
//    id: int, org: string, sale: double, id: int, org: string, sale: double
//    Project [id#3, org#4, sale#5, id#11, org#12, sale#13]
//    +- Filter (id#3 = id#11)
//    +- Join Inner
//    :- SubqueryAlias t1, `test`
//      :  +- LogicalRDD [id#3, org#4, sale#5]
//    +- SubqueryAlias t2, `test`
//    +- LogicalRDD [id#11, org#12, sale#13]
//
//    == Optimized Logical Plan ==
//    Join Inner, (id#3 = id#11)
//    :- Filter isnotnull(id#3)
//    :  +- LogicalRDD [id#3, org#4, sale#5]
//    +- Filter isnotnull(id#11)
//    +- LogicalRDD [id#11, org#12, sale#13]
//
//    == Physical Plan ==
//      *SortMergeJoin [id#3], [id#11], Inner
//    :- *Sort [id#3 ASC NULLS FIRST], false, 0
//      :  +- Exchange hashpartitioning(id#3, 200)
//    :     +- *Filter isnotnull(id#3)
//    :        +- Scan ExistingRDD[id#3,org#4,sale#5]
//    +- *Sort [id#11 ASC NULLS FIRST], false, 0
//    +- Exchange hashpartitioning(id#11, 200)
//    +- *Filter isnotnull(id#11)
//    +- Scan ExistingRDD[id#11,org#12,sale#13]

    q.explain()
  }
}
