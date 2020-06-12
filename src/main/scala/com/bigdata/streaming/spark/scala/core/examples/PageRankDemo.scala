package com.bigdata.streaming.spark.scala.core.examples

import org.apache.spark.sql.SparkSession
import org.junit.Test

class PageRankDemo {


  @Test
  def test(): Unit ={
    val argStr = "file:///E:\\studyAndTest\\data\\mllib\\pagerank_data.txt 10"
    main(argStr.split(" "))
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }

//    showWarning()

    val spark = SparkSession
      .builder
      .appName("SparkPageRank")
      .master("local[1]")
      .getOrCreate()

    val iters = if (args.length > 1) args(1).toInt else 10
    val lines = spark.read.textFile(args(0)).rdd
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.collect()
    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

    spark.stop()
  }

}
