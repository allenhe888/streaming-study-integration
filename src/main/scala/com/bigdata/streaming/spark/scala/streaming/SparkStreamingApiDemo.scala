package com.bigdata.streaming.spark.scala.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.bigdata.streaming.spark.scala.SSparkHelper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn.ApplicationMasterArguments
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.junit.Test

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class SparkStreamingApiDemo extends SSparkHelper with Serializable {



  @Test
  def testOperate()={
    val abc = 1L << 20
    println(abc)

  }


  @Test
  def testLocalQueueStreaming(): Unit ={
    val props = Map(
//      "spark.extraListeners" ->classOf[MySparkListenerImpl].getCanonicalName
      "spark.executor.heartbeatInterval" ->"20s"
    )

    val ssc = SparkStreamingApiDemo.getSparkStreamingContext(false,2,3000,props)
//    runQueueDStreamTest(ssc,List(1024,1024*100,1024*1024,1024*1024*10),5)
//    ssc.addStreamingListener(new MyStreamingListenerImpl)
    runQueueDStreamNoReduceTest(ssc,List(1024,1024*100,1024*1024,1024*1024*10),5)

  }

  @Test
  def testRemoteQueueStreaming(): Unit ={
    val ssc = SparkStreamingApiDemo.getSparkStreamingContext(true,1,3000)
//    ssc.sparkContext.setLogLevel(Level.WARN.toString)
//    runQueueDStreamTest(ssc,List(1024,1024*100,1024*1024,1024*1024*10),3)
    runQueueDStreamNoReduceTest(ssc,List(1024,1024*100,1024*1024,1024*1024*10),5)
  }










  // 本地测试SDC Kafka
  val topic = "HJQ_testKafkaPerPartition"
  val groupId = "sparkStreamingGroup_01"
//  @Test
//  def testKafkaDStreamSimpleWithMapFunc():Unit={
//    val ssc = SparkStreamingApiDemo.getSparkStreamingContext(false,2,5000,Map(
//      "spark.streaming.backpressure.enabled" ->"true",
//      "spark.streaming.kafka.maxRatePerPartition" ->"50"
//    ))
//
//    val kafkaProps = new mutable.HashMap[String,String]()
//    kafkaProps.put("bootstrap.servers", "ldsver51:9092")
//    kafkaProps.put("key.deserializer", classOf[StringDeserializer].getName)
//    kafkaProps.put("value.deserializer", classOf[StringDeserializer].getName)
//    kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
//    kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//    kafkaProps.put("group.id", groupId)
//
//
//
//    val kafkaDStream:DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
//        LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe(List(topic),kafkaProps))
//    val mapDS = kafkaDStream.map(record => (record.key(), record.value()))
//    kafkaDStream.foreachRDD(kafkaRDD=>{
//      val mapParts= kafkaRDD.mapPartitions( (it:Iterator[ConsumerRecord[String,String]])  =>{
//        val jsons = new ArrayBuffer[JSONObject]
//        while(it.hasNext){
//          val record:ConsumerRecord[String,String] = it.next()
//          jsons.+=(JSON.parseObject(record.value()))
//        }
//        Array("jsons.size="+jsons.size).iterator
//      })
//
//      mapParts.count()
//    })
//
//    ssc.start()
//    ssc.awaitTermination()
//
//  }

  @Test
  def debugLocalSdcKafkaDStream(): Unit ={
    val ssc = SparkStreamingApiDemo.getSparkStreamingContext(false,1,10000,Map(
      "spark.streaming.backpressure.enabled" ->"true",
      "spark.streaming.kafka.maxRatePerPartition" ->"50"

    ))

    runDebugKafkaDStreamTest(ssc,"ldsver51:9092",topic,groupId)
//    runDebugKafkaDStreamTest(ssc,"ldsver53:9092,ldsver53:9093,ldsver53:9094","HJQ_testKafkaPerPartition")

  }


//  @Test
//  def testKafkaRebalance():Unit={
//    val kafkaConsumer = KafkaDevHelper.getKafkaConsumer(ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, groupId))
//
//  }



  def runQueueDStreamTest(ssc: StreamingContext, nums: List[Int], partitonSize:Int ):Unit = {
    case class InputRecord(val key:String,val size:Int) {
      var data: Array[Byte] = _
    }
    case class GroupedRecord[T](key:String,records:List[T],val size:Int)
    case class PartitionData[K](index:Int, partitionData:List[K], val size:Int)

    val recordQueue = new mutable.Queue[RDD[InputRecord]]()
    val executorProcessSleepSec:Int = 4;
    var list:List[InputRecord] = List(
      new InputRecord("1",1024),
      new InputRecord("1",1024*100),
      new InputRecord("3",1024*1024)
      //      new InputRecord("4",1024*1024*1024)
    )

    if(nums!=null && nums.nonEmpty){
      val buff = new ArrayBuffer[InputRecord]()
      nums.foreach(num=>{{
        buff.+=(new InputRecord(""+num,num))
      }})
      list = buff.toList
    }

    recordQueue.enqueue(ssc.sparkContext.parallelize(list,partitonSize))
    val thread = new Thread {
      override def run() = try {
        val random = new Random()
        val size = list.size
        while (true){

          recordQueue.enqueue(ssc.sparkContext.parallelize(List(list(random.nextInt(size))),partitonSize))
          Thread.sleep(1000)
        }
      } catch {
        case e: Exception => throw new RuntimeException(e)
      }
    }
    thread.start()
    val format = new SimpleDateFormat("HH:mm:ss.SSS")
    ssc.queueStream(recordQueue,true)
      .map(x=>{
        val windowTimestamp:Long= (System.currentTimeMillis()/1000) * 1000
        (windowTimestamp,x)
      })
      //      .window(Seconds(6),Seconds(2))
      .cache()
      .groupByKey()
      .map(x=>{
        val list = x._2.toList
        GroupedRecord(format.format(new Date(x._1)),list,list.map(_.size).sum)
      })
      .persist(StorageLevel.DISK_ONLY)
      .foreachRDD(rdd=>{
        val result= rdd.mapPartitionsWithIndex((index,it)=>{
          val list = it.toList
          list.foreach(gr=>gr.records.foreach(record=>record.data = new Array[Byte](record.size)))

          System.err.println("分区_"+index+"开始休眠秒数: "+executorProcessSleepSec)
          Thread.sleep(1000* executorProcessSleepSec)
          System.err.println("分区_"+index+"休眠结束,继续运行! ")

          Array(PartitionData(index,list,list.map(_.size).sum)).iterator
        })
        val array = result.count()

//        println("一个Job计算执行完, result.collect()="+array.map(x=>x.index +":"+ x.partitionData.size).toString)
        println("一个Job计算执行完, result.count()="+array)

      })

    ssc.start()
    ssc.awaitTermination()

  }

  def runQueueDStreamNoReduceTest(ssc: StreamingContext, nums: List[Int], partitonSize:Int ):Unit = {
    case class InputRecord(val key:String,val size:Int) {
      var data: Array[Byte] = _
    }
    case class GroupedRecord[T](key:String,records:List[T],val size:Int)
    case class PartitionData[K](index:Int, partitionData:List[K], val size:Int)

    val recordQueue = new mutable.Queue[RDD[InputRecord]]()
    val executorProcessSleepSec:Int = 1000;
    var list:List[InputRecord] = List(
      new InputRecord("1",1024),
      new InputRecord("1",1024*100),
      new InputRecord("3",1024*1024)
      //      new InputRecord("4",1024*1024*1024)
    )

    if(nums!=null && nums.nonEmpty){
      val buff = new ArrayBuffer[InputRecord]()
      nums.foreach(num=>{{
        buff.+=(new InputRecord(""+num,num))
      }})
      list = buff.toList
    }

    recordQueue.enqueue(ssc.sparkContext.parallelize(list,partitonSize))
    val thread = new Thread {
      override def run() = try {
        val random = new Random()
        val size = list.size
        while (true){
          recordQueue.enqueue(ssc.sparkContext.parallelize(List(list(random.nextInt(size))),partitonSize))
          Thread.sleep(1000)
        }
      } catch {
        case e: Exception => throw new RuntimeException(e)
      }
    }
    thread.start()
    val formatWithMs = new SimpleDateFormat("HH:mm:ss.SSS")
    val format = new SimpleDateFormat("HH:mm:ss")

    case class MapRecord01(windowTimestamp:Long,data:InputRecord)
    case class MapRecord02(windowTimestamp:Long,data:InputRecord,formatWindowTime:String)
    case class IndexPartitionFunc03[T](partitionIndex:Int, recordSize:Int, currentTime:String,records:List[T])
    case class MapPartitionFunc04[T](partitionIndex:Int, outputStr:String)

    case class MapRecord04(partitionIndex:Int,outputStr:String)

    ssc.queueStream(recordQueue,true)
      .map(x=>{
        val windowTimestamp:Long= (System.currentTimeMillis()/1000) * 1000
        MapRecord01(windowTimestamp,x)
      })
      //      .window(Seconds(6),Seconds(2))
//      .cache()
      .persist(StorageLevel.DISK_ONLY)
//      .groupByKey()
      .map(x=>{
        val strTime = format.format(new Date((x.windowTimestamp)))
        MapRecord02(x.windowTimestamp,x.data,strTime)
      })
//      .persist(StorageLevel.DISK_ONLY)
      .foreachRDD(rdd=>{
        val result= rdd.mapPartitionsWithIndex((index,it)=>{
          val list = it.toList
          var result = IndexPartitionFunc03(index,list.size,formatWithMs.format(new Date()),list)
          Thread.sleep(executorProcessSleepSec+new Random().nextInt(1000))
//          if(list.size>0){
//            result = IndexPartitionFunc03(index,list.size,formatWithMs.format(new Date()),list)
//            System.err.println("分区_"+index+"开始休眠秒数: "+executorProcessSleepSec)
//            Thread.sleep(1000* executorProcessSleepSec)
//            System.err.println("分区_"+index+"休眠结束,继续运行! ")
//          }
          Array(result).iterator
        })

      result.foreach(x =>{
        println("分区内第1次foreach()打印: \t分区_"+x.partitionIndex+"  -> "+x)
      })

      val collectResult= result.mapPartitions(it=>{
        val list = it.toList
        var partition  = 100;
        var outputStr  = "";
        if(list.size>0){
          list.map(x=> partition=x.partitionIndex)
          outputStr = list.map(_.toString).mkString("; ")
        }
        val outputResult = MapPartitionFunc04(partition,outputStr)
        println("分区内第二次打印:\n\t分区_"+partition+" -> "+outputResult)
        Array(outputResult).iterator
      })
        .collect()

      System.err.println("\n Driver端 第3次打印: collectResult="+collectResult.map(x=>x.toString).mkString(",")+"\n")

      })

    ssc.start()
    ssc.awaitTermination()

  }


  def runKafkaDStreamTest(ssc: StreamingContext, bootstrapServers: String="ldsver51:9092", topic: String="HJQ_testKafkaPerPartition",maxRatePerPartition:Int=1000, groupId: String = "spark-kafka-"+Math.random()): Unit = {
    val kafkaProps = new mutable.HashMap[String,String]()
    kafkaProps.put("bootstrap.servers", bootstrapServers)
    kafkaProps.put("key.deserializer", classOf[StringDeserializer].getName)
    kafkaProps.put("value.deserializer", classOf[StringDeserializer].getName)
//    kafkaProps.put("auto.offset.reset", "latest")
    kafkaProps.put("auto.offset.reset", "earliest")
    kafkaProps.put("enable.auto.commit", "false")
    //    kafkaProps.put("max.poll.records", "100")

    kafkaProps.put("group.id", groupId)

    val kafkaDStream:DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe(List(topic),kafkaProps))

    SdcDriver.foreach(kafkaDStream)

    kafkaDStream.foreachRDD(rdd=>{
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val commiter = kafkaDStream.asInstanceOf[CanCommitOffsets]
      commiter.commitAsync(offsetRanges)

      offsetRanges.foreach(println(_))

    })


    ssc.start()
    ssc.awaitTermination()

  }

  def runDebugKafkaDStreamTest(ssc: StreamingContext, bootstrapServers: String="ldsver51:9092", topic: String="HJQ_testKafkaPerPartition", groupId: String = "spark-kafka-"+Math.random()): Unit = {
    val kafkaProps = new mutable.HashMap[String,String]()
    kafkaProps.put("bootstrap.servers", bootstrapServers)
    kafkaProps.put("key.deserializer", classOf[StringDeserializer].getName)
    kafkaProps.put("value.deserializer", classOf[StringDeserializer].getName)
    //    kafkaProps.put("auto.offset.reset", "latest")
    kafkaProps.put("auto.offset.reset", "earliest")
    kafkaProps.put("enable.auto.commit", "false")
    //    kafkaProps.put("max.poll.records", "100")
    kafkaProps.put("group.id", groupId)
//    kafkaProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "client_1-sparkStreaming-kafkaDebug")


    val kafkaDStream:DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe(List(topic),kafkaProps))

    SdcDriver.foreachDebug(kafkaDStream)

    kafkaDStream.foreachRDD(rdd=>{
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val commiter = kafkaDStream.asInstanceOf[CanCommitOffsets]
      commiter.commitAsync(offsetRanges)
      offsetRanges.foreach(println(_))
    })


    ssc.start()
    ssc.awaitTermination()

  }



}

object SparkStreamingApiDemo{
  lazy val instance: SparkStreamingApiDemo = new SparkStreamingApiDemo()

  def main(args: Array[String]): Unit = {

    var batchDuration = 3000;
    if(args.length>0){
      batchDuration = args(0).toInt
    }
    var executorNum = 1;
    if(args.length>1){
      executorNum = args(1).toInt
    }

    var simpleJobWithoutReduce = true;
    if(args.length>2){
      simpleJobWithoutReduce = args(2).toBoolean
    }

    val conf= getRemoteSparkConf()
    conf.set("spark.submit.deployMode", "cluster")

    // 设置Driver端- ApplicationMaster的 Debug & JMX
    conf.set("spark.driver.extraJavaOptions","-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=35053 "+
        "-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=10991"
      )

    val sdcConfigs= Map(
      "spark.streaming.backpressure.enabled" ->"true",
      "spark.streaming.blockInterval" ->"300000",
      "spark.streaming.dynamicAllocation.enabled" ->"false",
      "spark.streaming.dynamicAllocation.maxExecutors" ->"16",
      "spark.streaming.dynamicAllocation.minExecutors" ->"1",
      "spark.streaming.dynamicAllocation.scalingInterval" ->"300",

      "spark.streaming.kafka.consumer.poll.max.retries" ->"5",
      "spark.streaming.kafka.maxRatePerPartition" ->"100",
      "spark.yarn.queue" ->"default",
      //      "spark.yarn.principal" ->"app",
      "spark.yarn.am.waitTime" ->"300s",
      "spark.yarn.token.renewal.interval" ->"31536000000ms"
    )
    sdcConfigs.foreach(kv=>{
      conf.set(kv._1,kv._2)
    })

    conf.set("spark.executor.instances",executorNum.toString)


    val ssc = new StreamingContext(conf,Duration.apply(batchDuration))
    if(simpleJobWithoutReduce){
      instance.runQueueDStreamNoReduceTest(ssc,List(1024,1024*100,1024*1024,1024*1024*10),5)
    }else{
      instance.runQueueDStreamTest(ssc,List(1024,1024*100,1024*1024,1024*1024*10),5)
    }

    ssc.start()
    ssc.awaitTermination()


  }

  def getRemoteSparkConf(): SparkConf = {
    System.setProperty("HADOOP_USER_NAME", "app")

    val conf= new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setMaster("yarn")
      //      .set("spark.submit.deployMode","cluster")
      //      .set("spark.driver.extraJavaOptions","-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=35053 "+
      //        "-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=10991"
      //      )

      .set("spark.executor.extraJavaOptions","-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=35054 "+
        "-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=10992"
      )
      // 设置JMX
      //      .set("spark.driver.extraJavaOptions","-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Djava.rmi.server.hostname=ldsver52 -Dcom.sun.management.jmxremote.port=10992")
      //      .set("spark.executor.extraJavaOptions","-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=10992")

      // 设置resourcemanager的ip
      .set("yarn.resourcemanager.hostname","ldsver51")
      // 设置executor的个数
      .set("spark.executor.instances","1")
      // 设置executor的内存大小
      .set("spark.executor.memory", "1G")
      // 设置提交任务的yarn队列
      .set("spark.yarn.queue","default")
      // 设置driver的ip地址
//      .set("spark.driver.host","192.168.51.1")
      // 设置jar包的路径,如果有其他的依赖包,可以在这里添加,逗号隔开
      .set("spark.yarn.jars","hdfs:///sparkjars/*")

    conf
  }


  def getSparkStreamingContext(isRemote:Boolean,executorNum:Int =1,batchDuration:Int=5000, overParams:Map[String,String]=Map()): StreamingContext ={
    var conf: SparkConf= new SparkConf()
    if(isRemote){
      conf= getRemoteSparkConf()
      conf.set("spark.driver.host","192.168.51.1")
          .setJars(List(
            "E:\\ws\\ws-idea\\my-streaming-study\\streaming-study-integration\\target\\streaming-study-integration-1.0-SNAPSHOT.jar",
            //        "D:\\devRepo\\mvn_repo\\org\\apache\\spark\\spark-streaming-kafka-0-10_2.11\\2.2.0\\spark-streaming-kafka-0-10_2.11-2.2.0.jar", // 已经编译在study-integration源码中了;
            "D:\\devRepo\\mvn_repo\\org\\apache\\spark\\spark-network-yarn_2.11\\2.2.0\\spark-network-yarn_2.11-2.2.0.jar"
          ))
    }else{
      conf= new SparkConf()
        .setAppName(getClass.getSimpleName)
        .setMaster("local["+executorNum+"]")
    }


    val sdcConfigs= Map(
//      "spark.extraListeners" ->"com.bigdata.streaming.spark.scala.monitoring.MySparkListenerImpl",

      "spark.streaming.backpressure.enabled" ->"true",
      "spark.streaming.blockInterval" ->"300000",
      "spark.streaming.dynamicAllocation.enabled" ->"false",
      "spark.streaming.dynamicAllocation.maxExecutors" ->"16",
      "spark.streaming.dynamicAllocation.minExecutors" ->"1",
      "spark.streaming.dynamicAllocation.scalingInterval" ->"300",

      "spark.streaming.kafka.consumer.poll.max.retries" ->"5",
      "spark.streaming.kafka.maxRatePerPartition" ->"100",
      "spark.yarn.queue" ->"default",
//      "spark.yarn.principal" ->"app",
      "spark.yarn.am.waitTime" ->"300s",
      "spark.yarn.token.renewal.interval" ->"31536000000ms"
    )
    sdcConfigs.foreach(kv=>{
      conf.set(kv._1,kv._2)
    })

    conf.set("spark.executor.instances",executorNum.toString)
    if(overParams!=null && overParams.nonEmpty){
      overParams.foreach(kv=>{
        conf.set(kv._1,kv._2)
      })
    }

    new StreamingContext(conf,Duration.apply(batchDuration))
  }

  def getSparkPackages(): Unit ={
    new ApplicationMasterArguments(null);

  }

}
