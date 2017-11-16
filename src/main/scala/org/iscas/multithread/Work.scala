package org.iscas.multithread

import java.util.concurrent.{ExecutorService, Executors, ThreadPoolExecutor, TimeUnit}

import org.apache.spark.sql.SparkSession
import org.iscas.common.Consts

object Work {
  /**
    * 多线程提交任务
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("multi-thread").master(Consts.MASTER).getOrCreate()
    val pool: ExecutorService=Executors.newFixedThreadPool(2)
    pool.execute(new X("health",spark))
    pool.execute(new X("diagdesc",spark))
    while( !pool.awaitTermination(5,TimeUnit.SECONDS) )// 每隔5秒执行一次，询问线程池中的线程是否全部执行完成
    pool.shutdown()

  }
}

