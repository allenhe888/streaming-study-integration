package com.bigdata.streaming.spark.scala.streaming.kafkatest

import java.util

import com.alibaba.fastjson.JSON
import com.bigdata.streaming.common.CommonHelper
import com.bigdata.streaming.spark.java.streaming.MyRecord
import com.bigdata.streaming.spark.scala.SSparkHelper
import com.bigdata.streaming.spark.scala.streaming.SdcDriver._
import com.bigdata.streaming.spark.scala.streaming.SparkStreamingApiDemo
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.junit.Test

import scala.collection.mutable

class TestStreamSetsKafkaDStream extends SparkStreamingApiDemo {


  private var previousIncomingData: mutable.Queue[RDD[(String,String)]] = mutable.Queue()
  private var previousGeneratedRDDs: mutable.Queue[RDD[MyRecord]] = mutable.Queue()

  def sdcDriverForeachProcess(dstream:DStream[ConsumerRecord[String, String]],executorProcessSleepSec:Int = 4,processTime:Int = 5000,mbSize:Int=10): Unit ={

    dstream.foreachRDD(kafkaRDD =>{
      val kvRDD:RDD[(String,String)] = kafkaRDD.map(x=>(x.key(),x.value()))
      // 情况之前缓存的rdd队列;
      previousIncomingData.foreach((prevData:RDD[(String,String)]) => {
        // 将之前所有的RDD都移除磁盘;
        prevData.unpersist(false)
      })
      previousIncomingData.clear()
      // 清除上一个batch的 GeneratedRDDs 信息
      previousGeneratedRDDs.foreach((prevData:RDD[MyRecord]) => {
        prevData.unpersist(false)
      })
      previousGeneratedRDDs.clear()
      previousIncomingData += kvRDD // 将其添加进队列
      val incoming = kvRDD
      previousIncomingData += incoming

      var nextResult:RDD[MyRecord] = incoming.mapPartitions(it=>{ // nextResult封装的是Transformer的结果
        // 解析成(key,value)元组;
        val batch:List[(String,String)]= it.map(pair =>{
          val key = if(pair._1 ==null){// 获取key
            "UNKNOWN_PARTITION"
          }else {
            pair._1
          }
          (key,pair._2)  // 将key, value组成一个元组,返回
        }).toList
        println("this RDD batchSize= "+batch.size)
        val records:Iterator[MyRecord]= startBatch(batch)// 返回空的record

        // 非SDC的逻辑: 将String的Value转换成Json并封装MyRecord对象
        val myRecords = batch.map(x=>{
          val value = x._2
          val json = JSON.parseObject(x._2)
          new MyRecord(json.getString("orgId"),json.getString("modelId"),
            json.getString("assetId"),json.getString("pointId")
            ,json.getLong("time"),json.getDouble("value"),json.getInteger("dq"))
        })


        val lastTime = System.currentTimeMillis()
        var lists= new util.ArrayList[Any]()
        while (System.currentTimeMillis() - lastTime < processTime){//自定义的阻塞在此,循环生成固定大小数据的测试内存代码;
          val data=  CommonHelper.getInstance().dataGenerator.newFixBytesList(mbSize,0)
          lists.add(data)
          if(lists.size()>10){
            lists.clear()
            lists=null
            lists = new util.ArrayList[Any]()
          }
        }
        if(lists!=null && lists.size()>0 ){
          lists.clear()
          lists=null
        }
        myRecords.iterator
      })

      // 将transformer处理后的结果rdd 放入 previousGenerated队列
      previousGeneratedRDDs += nextResult

      // 先将transforms的RDD 触发并cache结果;
      // 非SDC,测试用逻辑: 休眠,并打印分区数据:
      if(executorProcessSleepSec>0){
        nextResult = nextResult.mapPartitionsWithIndex((index,it)=>{
          System.err.println("分区_"+index+"开始休眠秒数: "+executorProcessSleepSec)
          Thread.sleep(1000* executorProcessSleepSec)
          System.err.println("分区_"+index+"休眠结束,继续运行! ")
          it.toIterator
        })
      }


      nextResult
        //        .cache()
        .persist(StorageLevel.DISK_ONLY_2)
        .count()

      var id = 0
      transformers.foreach(transformer => {
        val result = transformer.transformer(nextResult)
        nextResult.count()

        nextResult = repartition(result)

        previousGeneratedRDDs += nextResult
        id += 1
      })

      // 再触发一次nextResult的计算;
      val records = nextResult.collect()
      println("records.size="+records.length)

    })

  }



  @Test
  def testLocalSdcKafkaDStream(): Unit ={
    val ssc = SparkStreamingApiDemo.getSparkStreamingContext(false,2,3000,Map(
      "spark.streaming.backpress" ->"true"
    ))
    runKafkaDStreamTest(ssc)
  }


  @Test
  def testRemoteSdcKafkaDStream(): Unit ={
    val ssc = SparkStreamingApiDemo.getSparkStreamingContext(true,1,5000,Map(
      "spark.streaming.backpressure.enabled" ->"true",
      "spark.streaming.kafka.maxRatePerPartition" ->"50"
    ))
    runKafkaDStreamTest(ssc)
  }

  @Test
  def testDebugLocalSdcKafkaDStream(): Unit ={
    val ssc = SparkStreamingApiDemo.getSparkStreamingContext(false,1,10000,Map(
      "spark.streaming.backpressure.enabled" ->"true",
      "spark.streaming.kafka.maxRatePerPartition" ->"100"
    ))

    val kafkaProps = new mutable.HashMap[String,String]()
    kafkaProps.put("bootstrap.servers", "ldsver51:9092")
    kafkaProps.put("key.deserializer", classOf[StringDeserializer].getName)
    kafkaProps.put("value.deserializer", classOf[StringDeserializer].getName)
    //    kafkaProps.put("auto.offset.reset", "latest")
    kafkaProps.put("auto.offset.reset", "earliest")
    kafkaProps.put("enable.auto.commit", "false")
    //    kafkaProps.put("max.poll.records", "100")
    kafkaProps.put("group.id", groupId)
//    kafkaProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(10000))
    kafkaProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(12000))
    //    kafkaProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "client_1-sparkStreaming-kafkaDebug")

    val topic = "testStringPerf"
    val kafkaDStream:DStream[ConsumerRecord[String, String]] = KafkaUtils
        .createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe(List(topic),kafkaProps))
    //SDC.Driver处理逻辑 + debug代码;
    sdcDriverForeachProcess(kafkaDStream,0,6000,50)
    //提交Offset
    kafkaDStream.foreachRDD(rdd=>{
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val commiter = kafkaDStream.asInstanceOf[CanCommitOffsets]
      commiter.commitAsync(offsetRanges)
      offsetRanges.foreach(println(_))
    })

    ssc.start()
    ssc.awaitTermination()
  }

//
//  def doKafkaDStreamCommitWithResetPolicy[K, V](kafkaDStream: DStream[ConsumerRecord[K, V]], rdd: RDD[ConsumerRecord[K, V]],
//                                                enableLoop:Boolean, reFromOffset:Long, maxPartitionOffset: Long): Unit = {
//    val offsetRanges = spark.rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//    val commiter = kafkaDStream.asInstanceOf[CanCommitOffsets]
//    var flag = false
//    val newOffsetRanges= offsetRanges.map(offsetRange=>{
//      if(offsetRange.untilOffset > maxPartitionOffset){
//        flag = true
//        OffsetRange.create(offsetRange.topic,offsetRange.partition,reFromOffset,offsetRange.untilOffset - offsetRange.fromOffset)
//      }else{
//        offsetRange
//      }
//    })
//    if(enableLoop && flag){
//      val directKafka = kafkaDStream.asInstanceOf[DirectKafkaInputDStream[String,String]]
//      directKafka.resetOffsetRange()
//    }
//    commiter.commitAsync(newOffsetRanges)
//    newOffsetRanges.foreach(println(_))
//  }

  @Test
  def testSdcKafkaDStreamWithSelfManagerOffsets(): Unit ={
    val ssc = SparkStreamingApiDemo.getSparkStreamingContext(false,1,10000,Map(
      "spark.streaming.backpressure.enabled" ->"true",
      "spark.streaming.kafka.maxRatePerPartition" ->"100"
    ))

    val kafkaProps = new mutable.HashMap[String,String]()
    kafkaProps.put("bootstrap.servers", "ldsver51:9092")
    kafkaProps.put("key.deserializer", classOf[StringDeserializer].getName)
    kafkaProps.put("value.deserializer", classOf[StringDeserializer].getName)
    //    kafkaProps.put("auto.offset.reset", "latest")
    kafkaProps.put("auto.offset.reset", "earliest")
    kafkaProps.put("enable.auto.commit", "false")
    //    kafkaProps.put("max.poll.records", "100")
    kafkaProps.put("group.id", groupId)
    //    kafkaProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(10000))
    kafkaProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(12000))
    //    kafkaProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "client_1-sparkStreaming-kafkaDebug")

    val topic = "testStringPerf"
    val reFromOffset = 0
    val enableLoop = true
    val kafkaDStream:DStream[ConsumerRecord[String, String]] = KafkaUtils
      .createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe(List(topic),kafkaProps))
    //SDC.Driver处理逻辑 + debug代码;
    sdcDriverForeachProcess(kafkaDStream,0,100,10)
    //提交Offset
    val maxPartitionOffset = 5000L
    kafkaDStream.foreachRDD(kafkaRDD=>{
//      val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
//      val commiter = kafkaDStream.asInstanceOf[CanCommitOffsets]
//      var flag = false
//      val newOffsetRanges= offsetRanges.map(offsetRange=>{
//        if(offsetRange.untilOffset > maxPartitionOffset){
//          flag = true
//          OffsetRange.create(offsetRange.topic,offsetRange.partition,reFromOffset,offsetRange.untilOffset - offsetRange.fromOffset)
//        }else{
//          offsetRange
//        }
//      })
//      if(enableLoop && flag){
//        val directKafka = kafkaDStream.asInstanceOf[DirectKafkaInputDStream[String,String]]
//        directKafka.resetOffsetRange()
//      }
//      commiter.commitAsync(newOffsetRanges)
//      newOffsetRanges.foreach(println(_))

      SSparkHelper.doKafkaDStreamCommitWithResetPolicy(kafkaDStream,kafkaRDD,true,0,5000L)
    })

    ssc.start()
    ssc.awaitTermination()
  }

//  def doKafkaDStreamCommitWithResetPolicy[K,V](kafkaDStream: DStream[ConsumerRecord[K,V]], kafkaRDD: RDD[ConsumerRecord[K,V]],
//                                                enableLoop:Boolean, reFromOffset:Long, maxPartitionOffset: Long): Unit = {
//    val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
//    val commiter = kafkaDStream.asInstanceOf[CanCommitOffsets]
//    var flag = false
//    val newOffsetRanges= offsetRanges.map(offsetRange=>{
//      if(offsetRange.untilOffset > maxPartitionOffset){
//        flag = true
//        OffsetRange.create(offsetRange.topic,offsetRange.partition,reFromOffset,offsetRange.untilOffset - offsetRange.fromOffset)
//      }else{
//        offsetRange
//      }
//    })
//    if(enableLoop && flag){
//      val directKafka = kafkaDStream.asInstanceOf[DirectKafkaInputDStream[K,V]]
//      directKafka.resetOffsetRange()
//    }
//    commiter.commitAsync(newOffsetRanges)
//    newOffsetRanges.foreach(println(_))
//  }


}
