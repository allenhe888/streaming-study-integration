package com.bigdata.streaming.spark.scala.ruoze.prestudy

import com.bigdata.streaming.common.CommKey
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class SparkPreStudy{

  val inputPath = "file:///E:\\studyAndTest\\inputs\\words.txt"
  val outputPath = "file:///E:\\studyAndTest\\outputs\\spark-idea-out.log"
  def getLocalSparkContext(): SparkContext ={
    val conf= new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)

    new SparkContext(conf)
  }

  @Test
  def testWorldCount(): Unit ={
    val sc = getLocalSparkContext()
    val line = sc.textFile("hdfs://ldsver53:9000/input/words.txt")
    val wordCount= line.flatMap(_.split(CommKey.EMPTY_STRING))
      .map((_,1))
      .reduceByKey((a,b)=>a+b)
    wordCount.foreach(println(_))
    val index = 3
    wordCount.saveAsTextFile("hdfs://ldsver53:9000/output/wcOutput-"+index)
    wordCount.saveAsTextFile("hdfs://ldsver53:9000/output/wcOutput_2-"+index)
    sc.stop()
  }

  @Test
  def testWorldCountReadMultiFiles(): Unit ={
    val sc = getLocalSparkContext()
    val line = sc.textFile("hdfs://ldsver53:9000/input")
    val wordCount= line.flatMap(_.split(CommKey.EMPTY_STRING))
      .map((_,1))
      .reduceByKey((a,b)=>a+b)
    wordCount.foreach(println(_))
    val index = 4
    wordCount.saveAsTextFile("hdfs://ldsver53:9000/output/wcOutput-"+index)
    wordCount.saveAsTextFile("hdfs://ldsver53:9000/output/wcOutput-"+index+"_02")
    sc.stop()
  }



}
