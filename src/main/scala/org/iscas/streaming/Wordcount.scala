package org.iscas.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.iscas.common.Consts

object Wordcount {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster(Consts.MASTER)
      .setAppName("Wordcount-stremaing")
    /**
      * 创建SparkStreamingContext,这个是SparkStreaming应用程序所有功能的起始点和程序调度的核心
      * SparkStreamingContext的构建可以基于SparkConf参数，也可基于持久化的SparkStreamingContext的内容来回复过来
      * （典型的场景是Driver崩溃后重新启动，由于Spark Streaming具有连续7*24小时不间断运行的特征，
      * 所有需要再Drver重新启动后继续上的状态，此时的状态恢复需要基于曾经的Checkpoint）
      *
      *
      * 在一个SparkStreaming应用程序中可以有多个SparkStreamingContext对象，使用下一个SparkStreaming程序之前
      * 需要把前面正在运行的SparkStreamingContext对象关闭掉，
      */
    val ssc=new StreamingContext(conf,Seconds(5))// batch interval=5s
    /**
      * 创建Spark Streaming 输入数据来源：input Stream
      * SliceJoin-1.数据输入来源可以基于File、HDFS、Flume、Kafka、Socket
      *
      * SliceJoin-2.在这里我们指定数据来源于网络Socket端口,
      * Spark Streaming连接上该端口并在运行的时候一直监听该端口的数据（当然该端口服务首先必须存在，并且在后续会根据业务需要不断的有数据产生）。
      *
      * 有数据和没有数据 在处理逻辑上没有影响（在程序看来都是数据）
      *
      * 3.如果经常在每间隔5秒钟 没有数据的话,不断的启动空的job其实是会造成调度资源的浪费。因为 并没有数据需要发生计算。
      * 所以企业级生产环境的代码在具体提交Job前会判断是否有数据，如果没有的话，就不再提交Job；
      */
    val lines=ssc.socketTextStream(Consts.IP,9999,storageLevel = StorageLevel.MEMORY_AND_DISK)
    /**
      * 我们就像对RDD编程一样，基于DStream进行编程。
      * DStream是RDD产生的模板，在Spark Streaming发生计算前，其实质是把每个Batch的DStream的操作翻译成为了RDD操作。
      * DStream对RDD进行了一次抽象。如同DataFrame对RDD进行一次抽象。
      */
    val words=lines.flatMap(e => e.split(" "))
    val wordCount=words.map(e => (e,1))
    wordCount.reduceByKey(_+_)
    /**
      * 此处的print并不会直接触发Job的执行，因为现在的一切都是在Spark Streaming框架的控制之下的。
      * 具体是否真正触发Job的运行是基于设置的Duration时间间隔的触发的。
      *
      *
      * Spark应用程序要想执行具体的Job对DStream就必须有output Stream操作。
      * ouput Stream 有很多类型的函数触发,类print、savaAsTextFile、saveAsHadoopFiles等，最为重要的是foreachRDD，因为Spark Streamimg处理的结果一般都会
      * 放在Redis、DB、DashBoard等上面，foreachRDD主要
      * 就是用来完成这些功能的，而且可以随意的自定义具体数据放在哪里。
      *
      */
    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true)

    /**
      * 启动程序后，通过在master上运行如下命令创建数据源：
      * nc -lk master 9999
      * 在命令行输入i love u
      */
  }
}
