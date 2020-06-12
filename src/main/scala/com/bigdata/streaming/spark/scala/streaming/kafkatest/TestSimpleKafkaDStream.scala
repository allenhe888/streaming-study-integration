package com.bigdata.streaming.spark.scala.streaming.kafkatest

import java.math.{BigDecimal, RoundingMode}
import java.util.concurrent.atomic.AtomicInteger

import com.bigdata.streaming.common.{CommKey, StaticCounter}
import com.bigdata.streaming.spark.scala.SSparkHelper
import com.bigdata.streaming.spark.scala.streaming.SparkStreamingApiDemo
import com.google.common.collect.{ImmutableMap}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.junit.Test

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class TestSimpleKafkaDStream extends SparkStreamingApiDemo {

  // 本地测试SDC Kafka
  val bootstrapServer = "ldsver51:9092"

  @Test
  def testKafkaDStreamSimpleWithMapFunc():Unit={
    val ssc = SparkStreamingApiDemo.getSparkStreamingContext(false,1,5000,Map(
      "spark.streaming.backpressure.enabled" ->"true",
      "spark.streaming.kafka.maxRatePerPartition" ->"50"
    ))

    val kafkaProps = new mutable.HashMap[String,String]()
    kafkaProps.put("bootstrap.servers", "ldsver51:9092")
    kafkaProps.put("key.deserializer", classOf[StringDeserializer].getName)
    kafkaProps.put("value.deserializer", classOf[StringDeserializer].getName)
    kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    kafkaProps.put("group.id", groupId)

    val kafkaDStream:DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe(List(topic),kafkaProps))
    val mapDS = kafkaDStream.map(record => (record.key(), record.value()))
    kafkaDStream.foreachRDD(kafkaRDD=>{
      val mapParts= kafkaRDD.mapPartitions( (it:Iterator[ConsumerRecord[String,String]])  =>{
        val jsons = new ArrayBuffer[String]
        while(it.hasNext){
          var record:ConsumerRecord[String,String] = null
          try{
            record = it.next()
          }catch {
            case e:AssertionError =>
              e.printStackTrace()
              val retryCounter = new AtomicInteger(0)
              while (record ==null && retryCounter.incrementAndGet() <=10){
                try{
                  record = it.next()
                }catch {
                  case e:AssertionError => None
                }
                println("重试次数: "+retryCounter.get())
              }
          }
//          val json = JSON.parseObject(record.value())
          jsons.+=(record.value())
        }
        val size = jsons.size
        jsons.clear()

        Array("jsons.size="+size).iterator
      })

      mapParts.count()
    })

    ssc.start()
    ssc.awaitTermination()

  }

  @Test
  def testKafkaDStream_4secBatchDuration_500PerPart_1mFetchByte():Unit={
    doSimpleKafkaDStreamPerfInLoop(6000,bootstrapServer,groupId,"testStringPerf",500,4200000,10000*500,1024*1024,4098,20,100)
  }

  @Test
  def testKafkaDStream_batch1s_2000PerPart_10mFetchByte():Unit={
    val argStr = "_batchDuration 1000 _bootstrapServers ldsver51:9092 _isRemote false _isClusterMode false " +
      "_maxRatePerPartition 50000 _maxPartitionOffset 10000000 _fetchMaxBytes 50485670 " +
      "_avgStartBatch 30 _qpsQueueSize 20 _executorNum 1 _executorMemory 1600M _enableBackPressure false"
    doMain(argStr.split(CommKey.EMPTY_STRING))
  }

  @Test
  def testKafkaOutOfRangeBug():Unit={
    val argStr = "_batchDuration 2000 _bootstrapServers ldsver51:9092 _isRemote false _isClusterMode false _maxPartitionOffset 4600000 _maxRatePerPartition 5000"
    doMain(argStr.split(CommKey.EMPTY_STRING))
//    doSimpleKafkaDStreamPerfInLoop(1000,bootstrapServer,groupId,"testStringPerf",1000,
//      4000000,10000*500,1024*1024*10,4098,20,100)
  }


  @Test
  def testEvictingSum(): Unit ={
//    val queue:EvictingQueue[java.lang.Double] = EvictingQueue.create(100)
//    queue.add(Math.random())
//    queue.add(Math.random())
//    queue.add(Math.random())
//    queue.add(Math.random())
//
//    val avg = collectHelper.calculateQueueSum(queue)
//    println(avg)
  }


  @Test
  def testDoMain(): Unit ={
    val argStr = "_batchDuration 500 _bootstrapServers ldsver51:9092 _isRemote false _enableBackPressure false"
    doMain(argStr.split(CommKey.EMPTY_STRING))
  }

  @Test
  def testYarnDriverMode(): Unit ={
    val argStr = "_batchDuration 500 _bootstrapServers ldsver51:9092 _isRemote true _isClusterMode false"
    doMain(argStr.split(CommKey.EMPTY_STRING))
  }

  @Test
  def testYarnClusterMode(): Unit ={
    doMain(Array())
  }


  def doMain(args: Array[String]): Unit = {

    val namespace = getNamespaceByArgsParser(args, ImmutableMap.builder[String, String]
      .put(CommKey.batchDuration, "6000")
      .put(CommKey.bootstrapServers, "ldsver51:9092")
      .put(CommKey.groupId, "spark-simple-kafka")
      .put(CommKey.topic, "testStringPerf")
      .put(CommKey.maxRatePerPartition, "500")
      .put(CommKey.reFromOffset, "0")
      .put(CommKey.maxPartitionOffset, (10000*800).toString())
      .put(CommKey.fetchMaxBytes, String.valueOf(1024*1024))
      .put(CommKey.consumerPollMs, String.valueOf(4098))
      .put(CommKey.avgStartBatch, String.valueOf(50))
      .put(CommKey.qpsQueueSize, String.valueOf(100))
      .put(CommKey.isRemote, "true")
      .put(CommKey.isClusterMode, "true")
      .put(CommKey.jars, "")
      .put(CommKey.executorNum, String.valueOf(1))
      .put(CommKey.executorMemory, "1600m")
      .put(CommKey.enableBackPressure, "false")
      .build(),true,"_")


    doSimpleKafkaDStreamPerfInLoop(
      namespace.getString(CommKey.batchDuration).toInt,
      namespace.getString(CommKey.bootstrapServers),
      namespace.getString(CommKey.groupId),
      namespace.getString(CommKey.topic),
      namespace.getString(CommKey.maxRatePerPartition).toInt,
      namespace.getString(CommKey.reFromOffset).toInt,
      namespace.getString(CommKey.maxPartitionOffset).toInt,
      namespace.getString(CommKey.fetchMaxBytes).toInt,
      namespace.getString(CommKey.consumerPollMs).toInt,
      namespace.getString(CommKey.avgStartBatch).toInt,
      namespace.getString(CommKey.qpsQueueSize).toInt,
      namespace.getString(CommKey.isRemote).toBoolean,
      namespace.getString(CommKey.isClusterMode).toBoolean,
      namespace.getString(CommKey.jars),
      namespace.getString(CommKey.executorNum).toInt,
      namespace.getString(CommKey.executorMemory),
      namespace.getString(CommKey.enableBackPressure).toBoolean
    )

  }


  def doSimpleKafkaDStreamPerfInLoop(batchDuration:Int,bootstrapServer:String,groupId:String,topic:String,maxRatePerPart:Int,
                                     reFromOffset:Long, maxPartitionOffset: Long,fetchMaxBytes:Int,consumerPollMs:Int=4098,
                                     avgStartBatch:Int=50,qpsQueueSize:Int=100,isRemote:Boolean=false,
                                     isClusterMode:Boolean=false,jarsString:String="",executorNum:Int=1,
                                     executorMemory:String="1024M",enableBackPressure:Boolean=false):Unit={

    var testJars:List[String] = jarsString.split(",").toList
    if(isRemote &&  jarsString.isEmpty){
      if(isClusterMode){
        testJars = List()
      }else if(jarsString.isEmpty){
        testJars = List(
          "E:\\ws\\ws-idea\\my-streaming-study\\streaming-study-integration\\target\\streaming-study-integration-1.0-SNAPSHOT.jar",
          "D:\\devRepo\\mvn_repo\\org\\apache\\spark\\spark-network-yarn_2.11\\2.2.0\\spark-network-yarn_2.11-2.2.0.jar"
        )
      }
    }


    val ssc = SSparkHelper.getSparkStreamingContext(isRemote,isClusterMode,testJars,executorNum,executorMemory,batchDuration,Map(
      "spark.streaming.backpressure.enabled" ->enableBackPressure.toString,
      "spark.streaming.kafka.consumer.poll.ms" ->consumerPollMs.toString,
      "spark.streaming.userdefine.batch.duration" ->batchDuration.toString,
      "spark.streaming.userdefine.avg.start.batch" ->avgStartBatch.toString,
      "spark.streaming.userdefine.qps.evict.queue.size" ->qpsQueueSize.toString,
      "spark.streaming.userdefine.record.size.per.job" ->(batchDuration/1000 * maxRatePerPart * 2).toString,
      "spark.extraListeners" ->classOf[KafkaSparkListener].getCanonicalName,//添加Task,Stage,Job级别的监控;
      "spark.streaming.kafka.maxRatePerPartition" ->maxRatePerPart.toString
    ))

    ssc.addStreamingListener(new KafkaStreamingListener(ssc)) //添加Streaming Batch级别的监控;

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
    val mapDS = kafkaDStream.map(record => (record.key(), record.value()))
    kafkaDStream.foreachRDD(kafkaRDD=>{
      val mapParts= kafkaRDD.mapPartitionsWithIndex((index:Int,it:Iterator[ConsumerRecord[String,String]])=>{
        val jsons = new ArrayBuffer[String]
        val startTime = System.nanoTime()
        if(SSparkHelper.stopAnExecutorTask(2, 0)){
          print(index)
        }
        StaticCounter.getInstance().createThreadTimeAndBatchCount()
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
        val endTime = System.nanoTime()
        jsons.clear()
        var elapsedTime = endTime-startTime


        val elapsedTimeMillis= elapsedTime/1000000
        if(elapsedTimeMillis >0){
          //          elapsedTime = 0
          printf("\n分区-%d, record=%d, elapsedTime=%d毫秒, qps=%d (%.3f 万/每秒) ",index,size,elapsedTimeMillis,size/elapsedTimeMillis,((size*0.1)/elapsedTimeMillis))
          // 计算 本批中 pollServer用时占总用时的 比重:
          val batchValue = StaticCounter.getInstance().getThreadBatchValue
          val fetchTimes = StaticCounter.getInstance().getThreadTimeValue
          val percent= BigDecimal.valueOf(fetchTimes * 100).divide(BigDecimal.valueOf(elapsedTime), 2, RoundingMode.HALF_UP).doubleValue
          println("本批: 发送Fetch请求次数" + batchValue + ", 运行总用时: " + elapsedTime / 1000000 + ", 其中sendFetch用时: " + fetchTimes / 1000000 + ", fetch用时占总比: " + percent + "%")



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


}


object TestSimpleKafkaDStream{

  def main(args: Array[String]): Unit = {
    new TestSimpleKafkaDStream().doMain(args)
  }

}