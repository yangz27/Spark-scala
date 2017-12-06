package org.iscas.iterativeBroadcastJoin.data

import org.apache.spark.sql.{SparkSession,SaveMode}
import org.iscas.iterativeBroadcastJoin.common.Config

object SkewedDataGenerator extends DataGenerator {
  /**
    *
    * @param spark
    * @param numberOfKeys key的个数
    * @param keysMultiplier 每个key下的数据量的乘积因子
    * @param numberOfPartitions 分区个数
    */
  def buildTestset(spark:SparkSession,
                   numberOfKeys:Int=Config.numberOfKeys,
                   keysMultiplier:Int=Config.keysMultiplier,
                   numberOfPartitions:Int=Config.numberOfPartitions): Unit ={

    println(s"# Generating skewed ${numberOfRows()} rows")

    import spark.implicits._

    val df=spark
      .sparkContext
      .parallelize(generateSkewedSequence(numberOfKeys),numberOfPartitions)// 生成kv序列，k是键值，v是键值重复的次数
      .flatMap(e => (0 until keysMultiplier).map(_=>e))// 每个list重复keysMultiplier次，增加数据规模
      .repartition(numberOfPartitions)
      .flatMap(pair => skewDistribution(pair._1,pair._2))// 生成num个key，并压扁
      .toDS()// 转换为dataset
      .map(Key)
      .repartition(numberOfPartitions)

    assert( df.count()==numberOfRows())

    println("# persist large table")
    df
      .write
      .mode(SaveMode.Overwrite)
      .save(Config.getLargeTableName("skewed"))

    // 创建中等规模表
    createMediumTable(spark,Config.getMediumTableName("skewed"),Config.getLargeTableName("skewed"),numberOfPartitions)

  }

  /**
    * 向序列中填充count个key
    * @param key
    * @param count
    * @return
    */
  def skewDistribution(key:Int,count:Int):Seq[Int]=Seq.fill(count)(key)

  def getMediumTableName:String=Config.getMediumTableName("skewed")

  def getLargeTableName:String=Config.getLargeTableName("skewed")

  case class KeyLabel(key: Int, label: String, pass: Int)

  case class Key(key:Int)
}
