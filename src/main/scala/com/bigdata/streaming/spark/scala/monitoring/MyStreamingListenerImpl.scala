package com.bigdata.streaming.spark.scala.monitoring

import com.bigdata.streaming.common.CommonHelper
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}

class MyStreamingListenerImpl extends StreamingListener with Logging{

  val jdbcHelper = CommonHelper.getJDBCHelper(3,"jdbc:mysql://ldsver51:3306/spark_monitor_db?characterEncoding=utf8","app","hq.2019.mysql");

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val batchInfo = batchCompleted.batchInfo


    println("------------------------ Spark Streaming实时计算监控-----------------------------")

    val str =
      s"""
         |batchTime:--------------------${batchInfo.batchTime}
         |outputOperationInfos:---------${batchInfo.outputOperationInfos}
         |processingStartTime:----------${batchInfo.processingStartTime}
         |processingEndTime:------------${batchInfo.processingEndTime}
         |numRecords:-------------------${batchInfo.numRecords}
         |---------------------------------- Batch Info---------------------------------
         |
         |""".stripMargin

    try{
      val connection = jdbcHelper.getConnection
      val sql = "insert into streaming_batch_info(batch_time,processing_start_time,processing_end_time,num_records) values(?,?,?,?)"

      val statement = connection.prepareStatement(sql)
      statement.setLong(1,batchInfo.batchTime.milliseconds)
      statement.setLong(2,batchInfo.processingStartTime.get)
      statement.setLong(3,batchInfo.processingEndTime.get)
      statement.setLong(4,batchInfo.numRecords)

      val res = statement.executeUpdate()
      if(res > 0){
//        println(str)
      }
      jdbcHelper.returnConnection(connection)
    }catch{
      case e:Exception => e.printStackTrace()
    }




  }



}
