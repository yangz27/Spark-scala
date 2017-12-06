package org.iscas.dataAwareJoin.data

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.iscas.dataAwareJoin.common.Config
import org.iscas.dataAwareJoin.data.SkewedDataGenerator.KeyLabel

import scala.util.Random


trait DataGenerator {

  /**
    * 统计生成数据的条数
    * 每个kv重复了keysMultiplier次
    * @param numberOfKeys
    * @param keysMultiplier
    * @return
    */
  def numberOfRows(numberOfKeys:Int=Config.numberOfKeys,
                   keysMultiplier:Int=Config.keysMultiplier):Long=
    generateSkewedSequence(numberOfKeys)
      .map(_._2)
      .sum*keysMultiplier
      .toLong

  /**
    * --生成kv序列
    * --v代表k重复的次数，v递减
    * --Scala的collection分为两类，比如对Seq，一类是普通的Seq，名字也叫Seq；一类是使用并行计算的Seq，名字叫ParSeq。其内部使用了Java的Fork/Join框架来做并行计算。
    * 比如List, Vector, Range都继承自Seq，因此它们的方法都非并行的，但是这些Seq有一个par方法可以将期转换为相应的ParSeq，ParSeq上的方法大多是并行的。
    * --比如numberOfKeys=3，结果[(0,3),(SliceJoin-1,SliceJoin-1),(SliceJoin-2,SliceJoin-1),(3,SliceJoin-1)]
    * @param numberOfKeys
    * @return
    */
  def generateSkewedSequence(numberOfKeys:Int):List[(Int,Int)]=
    (0 to numberOfKeys).par.map(
      i=>(
        i,
        math.ceil(
          (numberOfKeys.toDouble-i.toDouble)/(i.toDouble+1.0)
        ).toInt
      )
    ).toList

  /**
    * 创建中等规模表
    * 无论是skewed还是uniform，中等规模表相同
    * @param spark
    * @param tableName
    * @param largeTableName
    * @param numberOfPartitions
    */
  def createMediumTable(spark:SparkSession,
                        tableName:String,
                        largeTableName:String,
                        numberOfPartitions:Int):Unit={

    import spark.implicits._
    println("# create medium table")
    val df=
      spark
      .read
      .parquet(largeTableName)
      .as[Int]
        .distinct()
      .mapPartitions(rows => {
        val r=new Random()
        // 产生numberOfKeys个数据
        rows.map(
          key=>KeyLabel(
            key,
            s"Description for entry $key, that can be anything",
            Math.floor(r.nextDouble()*Config.numberOfBroadcastPasses).toInt// 标志第几个pass广播该数据
          )
        )
      })
      .repartition(numberOfPartitions)

    assert(df.count()==Config.numberOfKeys)

    println("# persist medium table")
    df
      .write
      .mode(SaveMode.Overwrite)
      .parquet(tableName)
  }

  /**
    * 创建测试数据
    * @param spark
    * @param numberOfKeys
    * @param keysMultiplier
    * @param numberOfPartitions
    */
  def buildTestset(spark:SparkSession,
                   numberOfKeys:Int=Config.numberOfKeys,
                   keysMultiplier:Int=Config.keysMultiplier,
                   numberOfPartitions:Int=Config.numberOfPartitions):Unit


  def getMediumTableName:String

  def getLargeTableName:String

}
