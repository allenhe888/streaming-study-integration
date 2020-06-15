package com.bigdata.streaming.spark.scala.streaming.queuedstream

import java.text.SimpleDateFormat
import java.util.Date

import com.bigdata.streaming.spark.scala.SSparkHelper
import com.bigdata.streaming.spark.scala.streaming.SparkStreamingApiDemo
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.junit.Test

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class QueueDStreamDemo  extends SSparkHelper with Serializable {


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



}
