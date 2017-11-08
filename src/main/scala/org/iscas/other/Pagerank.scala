package org.iscas.other

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object Pagerank {
  /**
    * pagerank算法
    * @param args
    */
  def main(args:Array[String]):Unit={
    val sc=SparkSession.builder().master("spark://133.133.30.12:7077").appName("page rank").getOrCreate()
    val links=sc.sparkContext.parallelize(
      List(
        ("A",List("B","C")),
        ("B",List("A","C")),
        ("C",List("A","B","D")),
        ("D",List("C"))
      )
    ).partitionBy(new HashPartitioner(10)).persist()

    var ranks=links.mapValues(v=>1.0)
    for( i <- 1 to 10 ){
      val contributions=links.join(ranks).flatMap{case(pageId,(links,rank))=>links.map(dest=>(dest,rank/links.size))}
      ranks=contributions.reduceByKey((x,y)=>x+y).mapValues(v=>0.15+0.85*v)
    }
    ranks.collect().foreach(println)

  }

}
