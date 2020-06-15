package com.bigdata.streaming.spark.scala

import java.sql.Timestamp

import com.alibaba.fastjson.JSON
import com.bigdata.streaming.common.CommKey
import com.bigdata.streaming.kafka.common.KafkaDevHelper
import com.bigdata.streaming.spark.java.SparkDevHelper
import com.google.common.collect.ImmutableMap
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, DirectKafkaInputDStream, HasOffsetRanges}
import org.apache.spark.streaming.{Duration, StreamingContext}

import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.RoundingMode

class SSparkHelper extends SparkDevHelper{

  val kafkaBootstrapServers = "kafka.bootstrap.servers"
  val subscribe = "subscribe"
  val includeTimestamp = "includeTimestamp"
  val SEPARATOR_EMTPY = " "

  val SOURCE_console = "console"
  val truncate = "truncate"
  val windowDuration = "6 seconds"
  val slideDuration = "3 seconds"



  def createSparkAndGetKafkaTimeValueDataset(topic: String, isRecord: Boolean): (SparkSession,Dataset[(String, Timestamp)]) = {
    val namespace = getArgsNamespaceByKVMapWithoutSeparator(ImmutableMap.builder[String, String]
      .put("master", "local[3]")
      .put("brokerServers", "ldsver51:9092")
      .put("topics", topic)
      .put("windowSize", "5")
      .put("slideSize", "1")
      .put("consumerGroup", "stream-kafka-group-recordWinAggr")
      .build)
    val windowDuration = namespace.getString("windowSize").toInt + " seconds"
    val slideDuration = namespace.getString("slideSize").toInt + " seconds"

    // 开始构建SparkSession回话
    val sparkSession = createSparkSession(namespace.getString("master"), Level.INFO)

    import sparkSession.implicits._
    val lines = sparkSession.readStream.format("kafka")
      .option(kafkaBootstrapServers, namespace.getString("brokerServers"))
      .option(subscribe, topic)
      .option(includeTimestamp, true)
      .load()
      .selectExpr("CAST(value AS STRING)","CAST(timestamp AS TIMESTAMP)")
      .as[(String,Timestamp)]

    val es = KafkaDevHelper.ProducerHelp.runProducerThreadInRate(topic, 10,
      namespace.getString("brokerServers"), 10000, isRecord)

    (sparkSession,lines)
  }


  def processDFByAggregatedWordCount(spark:SparkSession, lineDS: Dataset[(String, Timestamp)], windowDuration:String, slideDuration:String):Dataset[Row] = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val words = lineDS.flatMap(line=>{
      line._1.split(SEPARATOR_EMTPY).map((_,line._2))
    })
      .toDF("word","eventTime")
      .groupBy(
        window($"eventTime",windowDuration,slideDuration),$"word"
      )
      .count()
      .orderBy("window")

    words
  }

  def startOutputAndAwait(df: DataFrame) = {
    df.writeStream
      .outputMode(OutputMode.Complete())
      .format(SOURCE_console)
      .option(truncate,false)//缩短显示
      .start()
      .awaitTermination()
  }

  def startKafkaOutputAndAwait(df: DataFrame,topic:String,bootstrapServer:String = "ldsver51:9092") = {

    df.writeStream
      .format("kafka")
      .outputMode(OutputMode.Complete())
      .option(kafkaBootstrapServers,bootstrapServer)
      .option("topic",topic)
      .start()
      .awaitTermination()
  }

  def processDFByAggregatedRecordAvgValue(spark:SparkSession, lineDS: Dataset[(String, Timestamp)], windowDuration:String, slideDuration:String): Unit ={
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val winGroupedValues= getTimeValueDF(spark,lineDS)
      .groupBy(window($"time",windowDuration,slideDuration))

    val agg= winGroupedValues.agg(
      sum($"value").as("sumValue"),
      max($"value").as("maxValue")
    )
    agg.printSchema()
    startOutputAndAwait(agg)
  }


  def getTimeValueDF(spark:SparkSession, lineDS: Dataset[(String, Timestamp)]): DataFrame ={
    import spark.implicits._
    val timeValueDF =  lineDS
      .mapPartitions(it=>{
        val buff:ArrayBuffer[(Timestamp,Double)] = new ArrayBuffer[(Timestamp,Double)]
        it.foreach(tv=>{
          val json = JSON.parseObject(tv._1)
          val value:Double = json.getDouble("value")
          val time:Long = json.getLong("time")
          if(value!=null && time!=null){
            val timeValue:(Timestamp,Double) = (new Timestamp(time),value)
            buff += timeValue
          }
        })

        buff.iterator
      })
      .toDF("time","value")
    timeValueDF
  }

  def getAssetTimeValueDF(spark:SparkSession, lineDS: Dataset[(String, Timestamp)]): DataFrame ={
    import spark.implicits._
    val timeValueDF =  lineDS
      .mapPartitions(it=>{
        val buff:ArrayBuffer[(String,Timestamp,Double)] = new ArrayBuffer[(String,Timestamp,Double)]
        it.foreach(tv=>{
          val json = JSON.parseObject(tv._1)
          val value:Double = json.getDouble("value")
          val time:Long = json.getLong("time")
          val asset = json.getString("assetId")
          if(value!=null && time!=null && asset!=null){
            val timeValue:(String,Timestamp,Double) = (asset,new Timestamp(time),value)
            buff += timeValue
          }
        })

        buff.iterator
      })
      .toDF("asset","time","value")
    timeValueDF
  }


}

object SSparkHelper{

  var instance:SSparkHelper = new SSparkHelper()

  def doKafkaDStreamCommitWithResetPolicy[K, V](kafkaDStream: DStream[ConsumerRecord[K, V]], kafkaRDD: RDD[ConsumerRecord[K, V]],
                                                enableLoop:Boolean, reFromOffset:Long, maxPartitionOffset: Long): Unit = {
    val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
    val commiter = kafkaDStream.asInstanceOf[CanCommitOffsets]
    var flag = false
    val newOffsetRanges= offsetRanges.map(offsetRange=>{
      if(offsetRange.untilOffset > maxPartitionOffset){
        flag = true
//        OffsetRange.create(offsetRange.topic,offsetRange.partition,reFromOffset,offsetRange.untilOffset - offsetRange.fromOffset)
      }
      offsetRange
    })
    if(enableLoop && flag){
      val directKafka = kafkaDStream.asInstanceOf[DirectKafkaInputDStream[String,String]]
      directKafka.resetOffsetRange(reFromOffset)
    }
    commiter.commitAsync(newOffsetRanges)
//    newOffsetRanges.foreach(println(_))
  }

  def stopAnExecutorTask(taskNums:Int,taskIndex:Int):Boolean={
    val name = Thread.currentThread().getName
    if(name.startsWith("Executor task launch")){
      val splits = name.split(CommKey.EMPTY_STRING)
      if(splits(splits.length-1).toInt % taskNums==taskIndex){
        true
      }else{
        false
      }
    }else{
      false
    }
  }


  def getSparkStreamingContext(isRemote:Boolean,isClusterMode:Boolean=false,jars:List[String], executorNum:Int =1,
                               executorMemory:String="1024M", batchDuration:Int=5000,
                               overParams:Map[String,String]=Map()): StreamingContext ={
    var conf: SparkConf= new SparkConf()
      .setAppName(getClass.getSimpleName)
      .set("spark.executor.instances",executorNum.toString)
      .set("spark.executor.memory", executorMemory)
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.dynamicAllocation.enabled", "false")

    if(isRemote){
      conf.setMaster("yarn")
        .set("spark.yarn.queue","default")
        .set("spark.yarn.am.waitTime","300s")

      if(!jars.isEmpty){
        conf.setJars(jars)
      }
      if(!isClusterMode){ //如果是yarn-driver模式,
        conf
          .set("yarn.resourcemanager.hostname","ldsver51")
          .set("spark.yarn.queue","default")
          .set("spark.driver.host","192.168.51.1")
          .set("spark.yarn.jars","hdfs:///sparkjars/*")
      }

    }else{
      conf= new SparkConf()
        .setAppName(getClass.getSimpleName)
        .setMaster("local["+executorNum+"]")
    }

    if(overParams!=null && overParams.nonEmpty){
      overParams.foreach(kv=>{
        conf.set(kv._1,kv._2)
      })
    }

    new StreamingContext(conf,Duration.apply(batchDuration))
  }


  def qps(num:Double): Double ={
    BigDecimal(num)./(BigDecimal(10000)).setScale(2,RoundingMode.HALF_UP).doubleValue()
  }

  def qps(num:Double,b:Double): Double ={
    BigDecimal(num)./(BigDecimal(b)).setScale(2,RoundingMode.HALF_UP).doubleValue()
  }

  def qpsAsSecond(num:Double,b:Double): Double ={
    BigDecimal(num)./(BigDecimal(b)./(BigDecimal(1000))).setScale(2,RoundingMode.HALF_UP).doubleValue()
  }


}