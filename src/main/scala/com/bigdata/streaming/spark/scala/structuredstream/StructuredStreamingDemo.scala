package com.bigdata.streaming.spark.scala.structuredstream


import java.sql.Timestamp

import com.bigdata.streaming.spark.scala.SSparkHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.junit.Test

class StructuredStreamingDemo extends SSparkHelper with Serializable {


  @Test
  def testWordCountWindow(): Unit ={
    val (spark,ds) = createSparkAndGetKafkaTimeValueDataset("testKafkaApi",false)
    val wourdCount = processDFByAggregatedWordCount(spark,ds,"5 seconds","3 seconds")
    // 写出到Sink
    val query = wourdCount.writeStream
      .outputMode(OutputMode.Complete())
      .format(SOURCE_console)
      .option(truncate,false)//缩短显示
      .start()
    query.awaitTermination()

  }

  @Test
  def testWindowAggregatorBySumAndAvg(): Unit ={
    val (spark,ds) = createSparkAndGetKafkaTimeValueDataset("testStringPerf",true)
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val winGroupedValues= getTimeValueDF(spark,ds)
      .groupBy(window($"time",windowDuration,slideDuration))

    val agg= winGroupedValues.agg(
      sum($"value").as("sumValue"),
      max($"value").as("maxValue")
    )
    agg.printSchema()
    startOutputAndAwait(agg)
  }

  @Test
  def testWindowAggedDFToDatasetForMap(): Unit ={
    val (spark,dataSet) = createSparkAndGetKafkaTimeValueDataset("testStringPerf",true)
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val winGroupedValues= getAssetTimeValueDF(spark,dataSet)
      .groupBy(window($"time",windowDuration,slideDuration),$"asset")

    var df= winGroupedValues.agg(
      mean($"value").as("avgValue")
    )
        .select(
          $"window".getField("start").as("startTime"),
          $"asset",
          $"avgValue"
        )
        .as[(Timestamp,String,Double)]
    df.printSchema()

    val out = df.mapPartitions(it=>{
      val list = it.toList
      if(list.size>0){
        val tuple = list(0)
        val groupKey = tuple._2+"-"+tuple._1
        val values = list.map(_._3).mkString(" ")
        Array((groupKey,values)).iterator
      }else{
        Array().iterator
      }

    })
        .toDF("key","avgValues")
//        .select($"key",$"key"+"=="+$"avgValues".as("value"))

    out.printSchema()
//    out
//        .selectExpr("CAST(key as STRING)","CAST(avgValues AS STRING)")
//      .writeStream
//      .format("kafka")
////      .outputMode(OutputMode.Complete())
//      .option(kafkaBootstrapServers,"ldsver51:9092")
//      .option("topic","testSparkOut")
//      .start()
//      .awaitTermination()

    startOutputAndAwait(out)
  }


  @Test
  def testWindowAggedDFToDatasetForMapInKafka(): Unit ={
    val (spark,dataSet) = createSparkAndGetKafkaTimeValueDataset("testStringPerf",true)
    spark.conf.set(SQLConf.CHECKPOINT_LOCATION.key,"file:///E:/studyAndTest/sparkTemp")
//    spark.sparkContext.getConf.set(SQLConf.CHECKPOINT_LOCATION.key,"file:///E:/studyAndTest/sparkTemp")
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val winGroupedValues= getAssetTimeValueDF(spark,dataSet)
      .groupBy(window($"time",windowDuration,slideDuration),$"asset")

    var df= winGroupedValues.agg(
      mean($"value").as("avgValue")
    )
      .select(
        $"window".getField("start").as("startTime"),
        $"asset",
        $"avgValue"
      )
      .as[(Timestamp,String,Double)]
    df.printSchema()

    val out = df.mapPartitions(it=>{
      val list = it.toList
      if(list.size>0){
        val tuple = list(0)
        val groupKey = tuple._2+"-"+tuple._1
        val values = list.map(_._3).mkString(" ")
        Array((groupKey,values)).iterator
      }else{
        Array().iterator
      }

    })
      .toDF("key","value")
      .selectExpr("CAST(key as STRING)","CAST(value AS STRING)")

    out.printSchema()
//    startKafkaOutputAndAwait(out,"testSparkOut","ldsver51:9092")
    startOutputAndAwait(out)

  }


}



object StructuredStreamingDemo{

  def main(args: Array[String]): Unit = {
    new StructuredStreamingDemo().testWindowAggedDFToDatasetForMapInKafka()

  }


}
