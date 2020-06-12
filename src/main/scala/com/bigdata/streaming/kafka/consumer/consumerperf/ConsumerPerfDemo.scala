package com.bigdata.streaming.kafka.consumer.consumerperf

object ConsumerPerfDemo {

  var brokerList = "ldsver51:9092"
  var topic = "testKafkaApi"
  var group = "ideaConsumerGroup"
  var messages = String.valueOf(10000 * 100)

  def parseArgs(): Array[String] = {
    val params= List(
      "--broker-list",brokerList,
      "--topic",topic,
      "--group",group,
      "--messages",messages
    )
    params.toArray
  }



  def testConsumerPerformacne(args: Array[String]): Unit = {
    if(args.length>0){
      ConsumerPerformance.main(args)
    }else{
      ConsumerPerformance.main(parseArgs())
    }
  }


  def doMain(brokerList:String,topic:String,group:String,numRecord:Int):Unit={
    val params= List(
      "--broker-list",brokerList,
      "--topic",topic,
      "--group",group,
      "--messages",numRecord.toString,
      "--reporting-interval","1000",
      "--show-detailed-stats"
    )
    testConsumerPerformacne(params.toArray)
  }

  def doMain(brokerList:String,topic:String,group:String,numRecord:Int,recordSize:Int):Unit={
    val params= List(
      "--broker-list",brokerList,
      "--topic",topic,
      "--group",group,
      "--messages",numRecord.toString,
      "--record-size",recordSize.toString
    )
    testConsumerPerformacne(params.toArray)
  }

  def testConsumerPerfInThreeTopic():Unit ={
    val numRecords = 10000 * 500
    val recordByteSize = 210
    val byte100Topic = "testBytePerf100"
    val byteRecordTopic = "testBytePerfRecord"
    val stringTopic = "testStringPerf"

    val group = "testPerfGroup"


//    KafkaDevHelper.ConsumerHelp.previewRunningForResetBeginningOffsets(brokerList, stringTopic, group)
//    System.err.println("\n\nC.1 原生ConsumerPerf \t-\t topic=" + byte100Topic + " \t-\t recordSize=" + numRecords + " \t-\t byte[].size=" + 100)
//    ConsumerPerfDemo.doMain(brokerList, byte100Topic, group+"-perf-byte100", numRecords)

    System.err.println("\n\nC.2 原生ConsumerPerf \t-\t topic=" + byteRecordTopic + " \t-\t recordSize=" + numRecords + " \t-\t byte[].size=" + recordByteSize)
    ConsumerPerfDemo.doMain(brokerList, byteRecordTopic, group+"-perf-byteRecord", numRecords)

    System.err.println("\n\nC.3 原生ConsumerPerf \t-\t topic=" + stringTopic + " \t-\t recordSize=" + numRecords + " \t-\t byte[].size=" + recordByteSize)
    ConsumerPerfDemo.doMain(brokerList, stringTopic, group+"-perf-string", numRecords)


  }

  def main(args: Array[String]): Unit = {
//    testConsumerPerformacne(args)

    testConsumerPerfInThreeTopic()

  }


}
