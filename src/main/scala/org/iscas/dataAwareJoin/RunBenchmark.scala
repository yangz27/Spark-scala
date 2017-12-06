package org.iscas.dataAwareJoin


import org.apache.spark.sql.{SaveMode, SparkSession}
import org.iscas.common.Consts
import org.iscas.dataAwareJoin.common.Config
import org.iscas.dataAwareJoin.data.{DataGenerator, SkewedDataGenerator, UniformDataGenerator}
import org.iscas.dataAwareJoin.join._
import org.iscas.dataAwareJoin.join.{SliceJoin, SliceJoinType}

object RunBenchmark extends App {
  /**
    * 计算代码块的执行时间
    * @param block
    * @tparam R
    * @return
    */
  def time[R](block: => R):R={
    val t0=System.nanoTime()
    val result=block
    val t1=System.nanoTime()
    println("# Elapsed time: "+(t1-t0)/1000/1000/1000+" sec")
    result
  }

  def runTest(generator: DataGenerator,
              joinType: JoinType,
              tableNameOutput:String): Unit ={
    val rows=generator.numberOfRows()

    val name=s"$generator : $joinType, kyes=${Config.numberOfKeys}, multiplier=${Config.keysMultiplier}, rows=$rows "
    println(name)

    val spark=getSparkSession(name)

    time{
      val out=joinType match{
        case _:SortMergeJoinType => SortMergeJoin.join(
          spark,
          spark.read.load(generator.getLargeTableName),
          spark.read.load(generator.getMediumTableName)
        )
        case _:SliceJoinType => SliceJoin.join(// SliceJoinDetail显示算法的细节,SliceJoin是算法本身
          spark,
          spark.read.load(generator.getLargeTableName),
          spark.read.load(generator.getMediumTableName)
        )
        case _:IterativeBroadcastJoinType => IterativeBroadcastJoin.join(
          spark,
          spark.read.load(generator.getLargeTableName),
          spark.read.load(generator.getMediumTableName)
        )
      }
      out.write.mode(SaveMode.Overwrite).parquet(tableNameOutput)
//      println(out.queryExecution)
//      out.groupBy("key").count().collect().foreach(println)
//      println(s"result cnt is ${out.count()}")
    }

    spark.stop()
  }

  def runBenchmark(dataGenerator:DataGenerator,
                   iterations:Int=8,
                   outputTable:String="result.parquet"):Unit={
    // 保存最原始的规模因子
    val originalMultiplier=Config.keysMultiplier

    // 9次迭代，每次的数据规模都增加
    (0 to iterations).map(e => originalMultiplier+(e*originalMultiplier))
//    (0 to SliceJoin-1).map(e => originalMultiplier+(e*originalMultiplier))

      .foreach(multiplier=>{

        val keys=Config.numberOfKeys
        // 这里改变因子，dataGenerator使用的是Config内的变量，也会自动改变
        Config.keysMultiplier=multiplier

        val rows=dataGenerator.numberOfRows()

        val spark=getSparkSession(s"$dataGenerator : Generate dataset with $keys keys,$rows rows,$multiplier multiplier")

        dataGenerator.buildTestset(
          spark,
          keysMultiplier = multiplier
        )
        spark.stop()

        runTest(
          dataGenerator,
          new SortMergeJoinType,
          outputTable
        )

        runTest(
          dataGenerator,
          new SliceJoinType,
          outputTable
        )
//
//        runTest(
//          dataGenerator,
//          new IterativeBroadcastJoinType,
//          outputTable
//        )
//
      })

    Config.keysMultiplier=originalMultiplier
  }

  def getSparkSession(appName:String="Spark Application"):SparkSession={
    val spark=SparkSession
      .builder()
      .master(Consts.MASTER)
      .appName(appName)
      .getOrCreate()

    spark.conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("parquet.enable.dictionary","false")
    spark.conf.set("spark.default.parallelism",Config.numberOfPartitions)
    spark.conf.set("spark.sql.shuffle.partitions",Config.numberOfPartitions)

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

//  runBenchmark(UniformDataGenerator)
    runBenchmark(SkewedDataGenerator)
}
