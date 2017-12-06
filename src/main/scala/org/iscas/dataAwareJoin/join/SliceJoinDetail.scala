package org.iscas.dataAwareJoin.join

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.math.abs

object SliceJoinDetail extends JoinStrategy {

  /**
    *
    * @param spark
    * @param dfLarge
    * @param dfMedium
    * @return
    */
  private def sliceJoin(spark:SparkSession,
                        dfLarge:DataFrame,
                        dfMedium:DataFrame):DataFrame={
    // 明确地禁止内部使用broadcastjoin
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

    dfLarge.join(
      dfMedium,
      Seq("key"),
      "left_outer"
    ).select(
      dfLarge("key"),
      dfMedium("label")
    )

  }

  /**
    * 改进的join算法，slice join
    * @param spark
    * @param dfLarge
    * @param dfMedium
    * @return
    */
  override def join(spark: SparkSession,
                    dfLarge: DataFrame,
                    dfMedium: DataFrame): DataFrame = {
    val startTime=System.nanoTime()
    dfLarge.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // 无放回采样,生成频率直方图
    val distribution=dfLarge.sample(false,0.001).rdd
      .map(e=>(e(0),1))
      .reduceByKey((x,y)=>x+y)
      .collect()

    // 按照等频率分为两个bucket
    var key_part:Map[String,Boolean]=Map()
    var freq_part=0
    val all_freq=distribution.map(_._2.toInt).reduce((a,b) => a+b)
    var flag=true // 标志是否继续添加
    distribution.sortWith((a,b) => a._2.toInt>b._2.toInt)// 升序排列
      .foreach{e =>
      val k:String=e._1.toString
      val freq=e._2.toInt

      if( flag ){
        if( freq_part+freq<=0.5*all_freq ){
          freq_part+=freq
          key_part += (k -> true)
        }else{

          if( freq_part==0 || abs(freq_part-0.5*all_freq)>abs(freq_part+freq-0.5*all_freq) ){
            freq_part+=freq
            key_part += (k -> true)
          }

          flag=false
        }
      }
    }
    println(s"# bucket SliceJoin-1 has keys ${key_part.keys.size}")

    println("# part1 time: "+(System.nanoTime()-startTime)/1000/1000/1000+" sec")
    val startTime2=System.nanoTime()

    // split
    val key_part_bc=spark.sparkContext.broadcast(key_part)
    val dfLarge1=dfLarge.filter(e=> key_part_bc.value.get(e(0).toString)!=None )
    val dfLarge2=dfLarge.filter(e=> key_part_bc.value.get(e(0).toString)==None )
    println(dfLarge.count())
    println(dfLarge1.count())
    println(dfLarge2.count())

    val r1=sliceJoin(spark,dfLarge1,dfMedium)
    val r2=sliceJoin(spark,dfLarge2,dfMedium)

    val r=r1.union(r2)
//    dfLarge.unpersist()// join不是action算子,计算到函数之外的write才触发,因此还不能unpersist

    // 这里join还没触发,因此看不到时间
//    println("# part2 time: "+(System.nanoTime()-startTime2)/1000/1000/1000+" sec")

    r
  }
}
