package org.iscas.iterativeBroadcastJoin.data

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.iscas.iterativeBroadcastJoin.common.Config

import scala.util.Random

object UniformDataGenerator extends DataGenerator {
  def buildTestset(spark:SparkSession,
                   numberOfKeys:Int=Config.numberOfKeys,
                   keysMultiplier:Int=Config.keysMultiplier,
                   numberOfPartitions:Int=Config.numberOfPartitions):Unit={
    val numRows=numberOfRows()

    println(s"# Generating uniform $numRows rows")

    import spark.implicits._

    val df=spark
      .range(keysMultiplier)// e.g. [0,...,100]
      .repartition(numberOfPartitions)
      .mapPartitions(rows => {
        val r=new Random()
        val count=(numRows/keysMultiplier).toInt// 每个分区数据数量相同
        // 每个分区产生count个随机数，随机数的范围:[0,numberOfKeys-SliceJoin-1]
        rows.map(_ => (0 until count).map(_ => r.nextInt(numberOfKeys))).flatten
      })
      .map(Key)
      .repartition(numberOfPartitions)

    assert(df.count()==numberOfRows())
    println("# persist large table")
    df
      .write
      .mode(SaveMode.Overwrite)
      .save(Config.getLargeTableName("uniform"))

    // 创建中等规模表
    createMediumTable(spark,Config.getMediumTableName("uniform"),Config.getLargeTableName("uniform"),numberOfPartitions)
  }

  def getMediumTableName:String=Config.getMediumTableName("uniform")

  def getLargeTableName:String=Config.getLargeTableName("uniform")

  case class KeyLabel(key: Int, label: String, pass: Int)

  case class Key(key:Int)
}
