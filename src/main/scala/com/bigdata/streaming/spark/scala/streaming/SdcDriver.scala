package com.bigdata.streaming.spark.scala.streaming

import java.util

import com.alibaba.fastjson.JSON
import com.bigdata.streaming.common.CommonHelper
import com.bigdata.streaming.spark.java.streaming.MyRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object SdcDriver {

  val transformers: ArrayBuffer[SparkTransformer] = ArrayBuffer()

  class SparkTransformer{
    def transformer(rdd:RDD[MyRecord]):RDD[MyRecord]={
      rdd.map(x=>x)
      rdd
    }
  }


  case class SdcRecord(orgId:String,assetId:String,modelId:String,time:Long,value:Double,dq:Int){
    override def toString: String = super.toString


  }

  private var previousIncomingData: mutable.Queue[RDD[(String,String)]] = mutable.Queue()

  private var previousGeneratedRDDs: mutable.Queue[RDD[MyRecord]] = mutable.Queue()

  def startBatch(batch: List[(String, String)]):Iterator[MyRecord] = {
    List.empty[MyRecord].iterator
  }

  def foreach(dstream:DStream[ConsumerRecord[String, String]]): Unit ={

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

      // 将其添加进队列
      previousIncomingData += kvRDD

      // 有两个rdd了? 一样的?
      val incoming = kvRDD
      previousIncomingData += incoming

      // nextResult封装的是Transformer的结果
      var nextResult:RDD[MyRecord] = incoming.mapPartitions(it=>{
        // 解析成(key,value)元组;
        val batch:List[(String,String)]= it.map(pair =>{
          // 获取key
          val key = if(pair._1 ==null){
            "UNKNOWN_PARTITION"
          }else {
            pair._1
          }

          // 将key, value组成一个元组,返回
          (key,pair._2)
        }).toList

        println("this RDD batchSize= "+batch.size)

        // 返回空的record
        val records:Iterator[MyRecord]= startBatch(batch)

        // 非SDC的逻辑: 将String的Value转换成Json并封装MyRecord对象
        val myRecords = batch.map(x=>{
          val value = x._2
          val json = JSON.parseObject(x._2)
          new MyRecord(json.getString("orgId"),json.getString("modelId"),
            json.getString("assetId"),json.getString("pointId")
            ,json.getLong("time"),json.getDouble("value"),json.getInteger("dq"))
        })

        myRecords.iterator
      })


      // 将transformer处理后的结果rdd 放入 previousGenerated队列
      previousGeneratedRDDs += nextResult

      // 先将transforms的RDD 触发并cache结果;
      nextResult.cache().count()

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

  def foreachDebug(dstream:DStream[ConsumerRecord[String, String]]): Unit ={
    val executorProcessSleepSec:Int = 4;

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

      // 将其添加进队列
      previousIncomingData += kvRDD

      // 有两个rdd了? 一样的?
      val incoming = kvRDD
      previousIncomingData += incoming

      // nextResult封装的是Transformer的结果
      var nextResult:RDD[MyRecord] = incoming.mapPartitions(it=>{
        // 解析成(key,value)元组;
        val batch:List[(String,String)]= it.map(pair =>{
          // 获取key
          val key = if(pair._1 ==null){
            "UNKNOWN_PARTITION"
          }else {
            pair._1
          }

          // 将key, value组成一个元组,返回
          (key,pair._2)
        }).toList


        println("this RDD batchSize= "+batch.size)

        // 返回空的record
        val records:Iterator[MyRecord]= startBatch(batch)

        // 非SDC的逻辑: 将String的Value转换成Json并封装MyRecord对象
        val myRecords = batch.map(x=>{
          val value = x._2
          val json = JSON.parseObject(x._2)
          new MyRecord(json.getString("orgId"),json.getString("modelId"),
            json.getString("assetId"),json.getString("pointId")
            ,json.getLong("time"),json.getDouble("value"),json.getInteger("dq"))
        })

        val processTime = 10000
        val lastTime = System.currentTimeMillis()
        var lists= new util.ArrayList[Any]()
        while (System.currentTimeMillis() - lastTime < processTime){
          val data=  CommonHelper.getInstance().dataGenerator.newFixBytesList(10,0)
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
      nextResult = nextResult.mapPartitionsWithIndex((index,it)=>{

        System.err.println("分区_"+index+"开始休眠秒数: "+executorProcessSleepSec)
        Thread.sleep(1000* executorProcessSleepSec)
        System.err.println("分区_"+index+"休眠结束,继续运行! ")

        it.toIterator
      })

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


  var partitionCount = -1

  def repartition[T: ClassTag](rdd: RDD[T]) : RDD[T] = {
    if (rdd.partitions.length > partitionCount) {
      JavaRDD.fromRDD(rdd).coalesce(partitionCount).rdd
    } else if (rdd.partitions.length < partitionCount) {
      rdd.repartition(partitionCount)
    } else {
      rdd
    }
  }

}
