package com.bigdata.streaming.spark.scala.streaming.memorystudy

import java.util.concurrent.atomic.AtomicInteger

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bigdata.streaming.common.{CommKey, StaticCounter}
import com.bigdata.streaming.spark.scala.SSparkHelper
import com.bigdata.streaming.spark.scala.streaming.SparkStreamingApiDemo
import com.bigdata.streaming.spark.scala.streaming.kafkatest.KafkaSparkListener
import com.google.common.collect.ImmutableMap
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.junit.Test

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SparkMemoryTest extends SparkStreamingApiDemo{


  def doSimpleKafkaDStreamPerfInLoop(batchDuration:Int,bootstrapServer:String,groupId:String,topic:String,maxRatePerPart:Int,
                                     reFromOffset:Long, maxPartitionOffset: Long,fetchMaxBytes:Int,consumerPollMs:Int=4098,
                                     avgStartBatch:Int=50,qpsQueueSize:Int=100):Unit={
    val ssc = SparkStreamingApiDemo.getSparkStreamingContext(false,1,batchDuration,Map(
      "spark.streaming.backpressure.enabled" ->"true",
      "spark.streaming.kafka.consumer.poll.ms" ->consumerPollMs.toString,
      "spark.streaming.userdefine.batch.duration" ->batchDuration.toString,
      "spark.streaming.userdefine.avg.start.batch" ->avgStartBatch.toString,
      "spark.streaming.userdefine.qps.evict.queue.size" ->qpsQueueSize.toString,
      "spark.extraListeners" ->classOf[KafkaSparkListener].getCanonicalName,//添加Task,Stage,Job级别的监控;
      "spark.streaming.kafka.maxRatePerPartition" ->maxRatePerPart.toString
    ))
    //    ssc.addStreamingListener(new MyStreamingListenerImpl) //添加batch级别的监控;

    val kafkaProps = new mutable.HashMap[String,String]()
    kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    kafkaProps.put("key.deserializer", classOf[StringDeserializer].getName)
    kafkaProps.put("value.deserializer", classOf[StringDeserializer].getName)
    kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    kafkaProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(fetchMaxBytes))
    kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)


    val kafkaDStream:DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe(List(topic),kafkaProps))
//    val mapDS = kafkaDStream.map(record => (record.key(), record.value()))
    kafkaDStream.foreachRDD(kafkaRDD=>{
      val mapParts= kafkaRDD.mapPartitionsWithIndex((index:Int,it:Iterator[ConsumerRecord[String,String]])=>{
        val jsons = new ArrayBuffer[String]
        val startTime = System.currentTimeMillis()
        if(SSparkHelper.stopAnExecutorTask(2, 0)){
          print(index)
        }
        while(it.hasNext){
          var record:ConsumerRecord[String,String] = null
          try{
            record = it.next()
          }catch {
            case e:AssertionError =>
              e.printStackTrace()
              StaticCounter.getInstance().incrementAndPrintCurrentValue("executor-kafkaRDD-mapParition_AssertionError.count")
              val retryCounter = new AtomicInteger(0)
              while (record ==null){
                if(retryCounter.incrementAndGet() >1000) throw e
                try{
                  Thread.sleep(1000)
                  record = it.next()
                }catch {
                  case e:AssertionError => None
                  case e:InterruptedException => None
                }
                println("重试次数: "+retryCounter.get())
              }
          }
          //          val json = JSON.parseObject(record.value())
          jsons.+=(record.value())
        }
        val size = jsons.size
        val endTime = System.currentTimeMillis()
        jsons.clear()
        var elapsedTime = endTime-startTime

        if(elapsedTime >0){
          //          elapsedTime = 0
          printf("\n分区-%d, record=%d, elapsedTime=%d毫秒, qps=%d (%.3f 万/每秒) ",index,size,elapsedTime,size/elapsedTime,((size*0.1)/elapsedTime))
          // 计算 本批中 pollServer用时占总用时的 比重:

        }else{
          printf("\n分区-%d, record=%d, elapsedTime=%d毫秒 ",index,size,elapsedTime)

        }

        Array(size).iterator
      })

      mapParts.count()
      SSparkHelper.doKafkaDStreamCommitWithResetPolicy(kafkaDStream,kafkaRDD,true,reFromOffset,maxPartitionOffset)

    })

    ssc.start()
    ssc.awaitTermination()

  }


  def getKafkaDStream(batchDuration:Int,bootstrapServer:String,groupId:String,topic:String,maxRatePerPart:Int,
                                     reFromOffset:Long, maxPartitionOffset: Long,fetchMaxBytes:Int,consumerPollMs:Int=4098,
                                     avgStartBatch:Int=50,qpsQueueSize:Int=100):(StreamingContext,DStream[ConsumerRecord[String, String]])={
    val ssc = SparkStreamingApiDemo.getSparkStreamingContext(false,1,batchDuration,Map(
      "spark.streaming.backpressure.enabled" ->"true",
      "spark.streaming.kafka.consumer.poll.ms" ->consumerPollMs.toString,
      "spark.streaming.userdefine.batch.duration" ->batchDuration.toString,
      "spark.streaming.userdefine.avg.start.batch" ->avgStartBatch.toString,
      "spark.streaming.userdefine.qps.evict.queue.size" ->qpsQueueSize.toString,
      "spark.extraListeners" ->classOf[KafkaSparkListener].getCanonicalName,//添加Task,Stage,Job级别的监控;
      "spark.streaming.kafka.maxRatePerPartition" ->maxRatePerPart.toString
    ))
    //    ssc.addStreamingListener(new MyStreamingListenerImpl) //添加batch级别的监控;

    val kafkaProps = new mutable.HashMap[String,String]()
    kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    kafkaProps.put("key.deserializer", classOf[StringDeserializer].getName)
    kafkaProps.put("value.deserializer", classOf[StringDeserializer].getName)
    kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    kafkaProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(fetchMaxBytes))
    kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    val kafkaDStream:DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe(List(topic),kafkaProps))
    (ssc,kafkaDStream)
  }


  def doMain(args: Array[String]): Unit = {
    val namespace = getNamespaceByArgsParser(args, ImmutableMap.builder[String, String]
      .put(CommKey.batchDuration, "6000")
      .put(CommKey.bootstrapServers, "ldsver51:9092")
      .put(CommKey.groupId, "spark-simple-kafka")
      .put(CommKey.topic, "testStringPerf")
      .put(CommKey.maxRatePerPartition, "500")
      .put(CommKey.reFromOffset, "0")
      .put(CommKey.maxPartitionOffset, (10000*500).toString())
      .put(CommKey.maxPartitionFetchBytes, String.valueOf(1024*1024))
      .put(CommKey.consumerPollMs, String.valueOf(4098))
      .put(CommKey.avgStartBatch, String.valueOf(50))
      .put(CommKey.qpsQueueSize, String.valueOf(100))
      .build)
    doSimpleKafkaDStreamPerfInLoop(
      namespace.getString(CommKey.batchDuration).toInt,
      namespace.getString(CommKey.bootstrapServers),
      namespace.getString(CommKey.groupId),
      namespace.getString(CommKey.topic),
      namespace.getString(CommKey.maxRatePerPartition).toInt,
      namespace.getString(CommKey.reFromOffset).toInt,
      namespace.getString(CommKey.maxPartitionOffset).toInt,
      namespace.getString(CommKey.maxPartitionFetchBytes).toInt,
      namespace.getString(CommKey.consumerPollMs).toInt,
      namespace.getString(CommKey.avgStartBatch).toInt,
      namespace.getString(CommKey.qpsQueueSize).toInt
    )
  }

  def doMainGetKafkaDStream(args: Array[String]):(StreamingContext,DStream[ConsumerRecord[String, String]]) = {
    val namespace = getNamespaceByArgsParser(args, ImmutableMap.builder[String, String]
      .put(CommKey.batchDuration, "6000")
      .put(CommKey.bootstrapServers, "ldsver51:9092")
      .put(CommKey.groupId, "spark-simple-kafka")
      .put(CommKey.topic, "testStringPerf")
      .put(CommKey.maxRatePerPartition, "500")
      .put(CommKey.reFromOffset, "0")
      .put(CommKey.maxPartitionOffset, (10000*500).toString())
      .put(CommKey.maxPartitionFetchBytes, String.valueOf(1024*1024))
      .put(CommKey.consumerPollMs, String.valueOf(4098))
      .put(CommKey.avgStartBatch, String.valueOf(50))
      .put(CommKey.qpsQueueSize, String.valueOf(100))
      .build)

    getKafkaDStream(
      namespace.getString(CommKey.batchDuration).toInt,
      namespace.getString(CommKey.bootstrapServers),
      namespace.getString(CommKey.groupId),
      namespace.getString(CommKey.topic),
      namespace.getString(CommKey.maxRatePerPartition).toInt,
      namespace.getString(CommKey.reFromOffset).toInt,
      namespace.getString(CommKey.maxPartitionOffset).toInt,
      namespace.getString(CommKey.maxPartitionFetchBytes).toInt,
      namespace.getString(CommKey.consumerPollMs).toInt,
      namespace.getString(CommKey.avgStartBatch).toInt,
      namespace.getString(CommKey.qpsQueueSize).toInt
    )
  }

  @Test
  def testDoMain(): Unit ={
    val argStr = "--batchDuration 5000 --bootstrapServers ldsver51:9092"

    doMain(argStr.split(CommKey.EMPTY_STRING))
  }


  /**
    * 测试Shuffle算子对内存占用的区别:
    *   - groupBy 算子没有map端的合并, 更占内存;
    *   - reduce()算子有map端的合并, 节省内存;
    */

  @Test
  def testGroupBy(): Unit ={
    val argStr = "--batchDuration 5000 --bootstrapServers ldsver51:9092"

    val (ssc,kafkaDStream) = doMainGetKafkaDStream(argStr.split(CommKey.EMPTY_STRING))

    // 1. 先Map操作
    kafkaDStream.foreachRDD(kafkaRDD=>{
      val mapParts= kafkaRDD.mapPartitionsWithIndex((index:Int,it:Iterator[ConsumerRecord[String,String]])=>{
        val jsons = new ArrayBuffer[JSONObject]
        val startTime = System.currentTimeMillis()
        if(SSparkHelper.stopAnExecutorTask(2, 0)){
          print(index)
        }
        while(it.hasNext){
          var record:ConsumerRecord[String,String] = null
          try{
            record = it.next()
          }catch {
            case e:AssertionError =>
              e.printStackTrace()
              StaticCounter.getInstance().incrementAndPrintCurrentValue("executor-kafkaRDD-mapParition_AssertionError.count")
              val retryCounter = new AtomicInteger(0)
              while (record ==null){
                if(retryCounter.incrementAndGet() >1000) throw e
                try{
                  Thread.sleep(1000)
                  record = it.next()
                }catch {
                  case e:AssertionError => None
                  case e:InterruptedException => None
                }
                println("重试次数: "+retryCounter.get())
              }
          }
           val json = JSON.parseObject(record.value())
          jsons.+=(json)
        }
        val size = jsons.size
        val endTime = System.currentTimeMillis()
        jsons.clear()
        var elapsedTime = endTime-startTime
        if(elapsedTime >0){
          //          elapsedTime = 0
          printf("\n分区-%d, record=%d, elapsedTime=%d毫秒, qps=%d (%.3f 万/每秒) ",index,size,elapsedTime,size/elapsedTime,((size*0.1)/elapsedTime))
          // 计算 本批中 pollServer用时占总用时的 比重:
        }else{
          printf("\n分区-%d, record=%d, elapsedTime=%d毫秒 ",index,size,elapsedTime)
        }
        jsons.iterator
      })


      mapParts.count()
//      SSparkHelper.doKafkaDStreamCommitWithResetPolicy(kafkaDStream,kafkaRDD,true,reFromOffset,maxPartitionOffset)

    })

    ssc.start()
    ssc.awaitTermination()

  }



}
