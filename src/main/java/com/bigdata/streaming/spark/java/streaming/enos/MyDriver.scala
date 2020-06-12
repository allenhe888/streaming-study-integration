package com.bigdata.streaming.spark.java.streaming.enos

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object MyDriver {

  val transformers: ArrayBuffer[SparkTransformer] = ArrayBuffer()

  class SparkTransformer{
    def transformer(rdd:RDD[SdcRecord]):RDD[SdcRecord]={
      rdd.map(x=>x)
      rdd
    }
  }

  case class SdcRecord(orgId:String,assetId:String,modelId:String,time:Long,value:Double,dq:Int)

  private var previousIncomingData: mutable.Queue[RDD[(String,String)]] = mutable.Queue()

  private var previousGeneratedRDDs: mutable.Queue[RDD[SdcRecord]] = mutable.Queue()

  def startBatch(batch: List[(String, String)]):Iterator[SdcRecord] = {
    List.empty[SdcRecord].iterator
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
      previousGeneratedRDDs.foreach((prevData:RDD[SdcRecord]) => {
        prevData.unpersist(false)
      })
      previousGeneratedRDDs.clear()

      // 将其添加进队列
      previousIncomingData += kvRDD

      // 有两个rdd了? 一样的?
      val incoming = kvRDD
      previousIncomingData += incoming

      // nextResult封装的是Transformer的结果
      var nextResult:RDD[SdcRecord] = incoming.mapPartitions(it=>{
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
        val records:Iterator[SdcRecord]= startBatch(batch)
        records
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
      nextResult.count()


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
