package org.iscas.dataAwareJoin.common

import com.typesafe.config.ConfigFactory

/**
  * 默认加载classpath下的application.conf,application.json和application.properties文件。
  */
object Config {
  private val conf=ConfigFactory.load()

  // broadcast的遍数
  var numberOfBroadcastPasses:Int=conf.getInt("broadcast.passes")

  var broadcastIterationTableName:String="tmp_broadcast_table.parquet"

  var numberOfPartitions:Int=conf.getInt("generator.partitions")

  var numberOfKeys:Int=conf.getInt("generator.keys")

  var keysMultiplier:Int=conf.getInt("generator.multiplier")

  def getMediumTableName(generatorType:String):String={
    conf.getString(s"generator.$generatorType.mediumTableName")
  }

  def getLargeTableName(generatorType:String):String={
    conf.getString(s"generator.$generatorType.largeTableName")
  }

}
