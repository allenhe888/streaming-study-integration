package com.bigdata.streaming.spark.scala.streaming.kafkatest

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.bigdata.streaming.common.CommonHelper
import org.apache.spark._
import org.apache.spark.scheduler._

import scala.math.BigDecimal.RoundingMode

class KafkaSparkListener(sparkConf:SparkConf) extends SparkListener {
  private val taskKafkaMessages:Int={
    val maxRatePerPartition = sparkConf.get("spark.streaming.kafka.maxRatePerPartition","500").toInt
    val batchDuration = sparkConf.get("spark.streaming.userdefine.batch.duration","5000").toInt
    batchDuration/1000 * maxRatePerPartition
  }

  private val recordSizePerJob:Int=  sparkConf.get("spark.streaming.userdefine.record.size.per.job","10000").toInt


  private val avgStartBatch = sparkConf.get("spark.streaming.userdefine.avg.start.batch","20").toInt
  private val qpsEvictQueueSize = sparkConf.get("spark.streaming.userdefine.qps.evict.queue.size","100").toInt

  def getTaskTypeNum(taskType:String): Int ={
    if(taskType.equalsIgnoreCase("ResultTask")){
      0
    }else if(taskType.equalsIgnoreCase("ShuffleMapTask")){
      1
    }else{
      -1
    }
  }
  def incrementAndGet(count:AtomicInteger):Int={
    count.incrementAndGet()
  }


  val batchCounter = new AtomicLong(0)
//  val distinctQueue:EvictingQueue[String] = EvictingQueue.create(100)
//  def addIfNotExists(key:String):Boolean = {
//    if(distinctQueue.contains(key)){
//      false
//    }else{
//      distinctQueue.add(key)
//      true
//    }
//  }


  def generateTaskAttemptKey(stageId:Int,stageAttemptId:Int,info:TaskInfo):String={
    Array("taskAttempt",stageId,stageAttemptId,info.id,info.attemptNumber).mkString("_")
  }


  // 进行Task 级别的监控
  val taskAttemptStart:ConcurrentHashMap[String,Long] = new ConcurrentHashMap[String,Long]()
  val taskQpsStat = CommonHelper.getInstance().collectHelper.createQueue(qpsEvictQueueSize)
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val info = taskStart.taskInfo
    val taskAttemptKey = generateTaskAttemptKey(taskStart.stageId,taskStart.stageAttemptId,info)
    taskAttemptStart.put(taskAttemptKey,System.currentTimeMillis())
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    try{
      onTaskEndImpl(taskEnd)
    }catch {
      case e:Exception => e.printStackTrace()
    }
  }
  def onTaskEndImpl(taskEnd: SparkListenerTaskEnd): Unit ={
    val info = taskEnd.taskInfo
    if (info != null && taskEnd.stageAttemptId != -1 && taskEnd.taskInfo.successful) {
      val taskAttemptKey = generateTaskAttemptKey(taskEnd.stageId,taskEnd.stageAttemptId,info)
      //作业Fail,kill,发生异常,将发送预警信息
//      if(!addIfNotExists(taskAttemptKey)) return //若返回false,表示该key之前存在过,这里就不做统计了;

      //计算task用时;
      val listenerStartTime = taskAttemptStart.get(taskAttemptKey)
      var listenedDuration:Long = 0L
      if(listenerStartTime!=null){ //ListenedDuration
        listenedDuration = System.currentTimeMillis() - listenerStartTime
        if(listenedDuration<=0) listenedDuration = info.duration
        taskAttemptStart.remove(taskAttemptKey)
        if(taskAttemptStart.size>10) taskAttemptStart.clear()
      }else{
        listenedDuration = info.duration
      }
      val duration = info.duration

      val inputRateSecByListener:Double= CommonHelper.divideRateAsSecond(taskKafkaMessages,listenedDuration,2)
      val inputRateSecByDuration:Double= CommonHelper.divideRateAsSecond(taskKafkaMessages,duration,2)
      var  averageTaskQps = 0.0
      if(batchCounter.get()>avgStartBatch){
        taskQpsStat.add(inputRateSecByDuration)
        var sum = CommonHelper.getInstance().collectHelper.calculateQueueSum(taskQpsStat)
        averageTaskQps = sum/taskQpsStat.size()
      }

      val metrics = taskEnd.taskMetrics
      val str =
        s"""
           |task listenedDuration:      ${listenedDuration}
           |task duration:              ${duration}   耗时分析: RunTime:${metrics.executorRunTime}, DeserTime:${metrics.executorDeserializeTime}, DeserCpuTime:${metrics.executorDeserializeCpuTime}, resultSerTime:${metrics.resultSerializationTime}, jvmGCTime:${metrics.jvmGCTime}
           |task kafkaNum:              ${taskKafkaMessages}
           |task inputRateSecByListener:${inputRateSecByListener}-- ${inputRateSecByListener/10000}万/秒
           |task inputRateSecByDuration:${inputRateSecByDuration}-- ${inputRateSecByDuration/10000}万/秒
           |task averageTaskQps:        ${averageTaskQps}--         ${averageTaskQps/10000}万/秒
           |--------------------------------------------------------------------------------------
           |""".stripMargin
      println(str)

    }

  }


  // 进行Stage级别的监控
  val stageAttempts:ConcurrentHashMap[String,Long] = new ConcurrentHashMap[String,Long]()
  val stageQpsStat = CommonHelper.getInstance().collectHelper.createQueue(qpsEvictQueueSize)
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageInfo = stageSubmitted.stageInfo
    if(stageInfo!=null && stageInfo.attemptId != -1 ){
      val stageAttemptKey = Array(stageInfo.stageId,stageInfo.attemptId).mkString("-")
      stageAttempts.put(stageAttemptKey,System.nanoTime())
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    if(stageInfo!=null && stageInfo.attemptId != -1 ){

      val stageAttemptKey = Array(stageInfo.stageId,stageInfo.attemptId).mkString("-")

      if(stageAttempts.containsKey(stageAttemptKey)){
        var listenedDuration = (System.nanoTime() - stageAttempts.get(stageAttemptKey))/1000000.00
        if(listenedDuration>0){
          val stageInputSize = taskKafkaMessages * stageInfo.numTasks
          val listenerStageQps:Double= CommonHelper.divideRateAsSecond(stageInputSize,listenedDuration,2)
          val metrics = stageInfo.taskMetrics
          val duration = stageInfo.completionTime.get - stageInfo.submissionTime.get
          if(duration>0){
//            val inputRateSecByDuration:Double= CommonHelper.divideRateAsSecond(stageInputSize,duration,2)
            val inputRateSecByDuration:Double= CommonHelper.divideRateAsSecond(stageInputSize,duration,2)
            var  averageStageQps = 0.0
            if(batchCounter.get()> avgStartBatch){
              stageQpsStat.add(inputRateSecByDuration)
              var sum = CommonHelper.getInstance().collectHelper.calculateQueueSum(stageQpsStat)
              averageStageQps = qps(sum,stageQpsStat.size())
            }
            val str =
              s"""
                 |stage listenedDuration:      ${listenedDuration}毫秒
                 |stage duration:              ${duration}   stage耗时分析: RunTime:${metrics.executorRunTime}, DeserTime:${metrics.executorDeserializeTime}, DeserCpuTime:${metrics.executorDeserializeCpuTime}, resultSerTime:${metrics.resultSerializationTime}, jvmGCTime:${metrics.jvmGCTime}
                 |stage stageInputSize:        ${stageInputSize}
                 |stage listenerStageQps:      ${listenerStageQps}-- ${qps(listenerStageQps)}万/秒
                 |stage inputRateSecByDuration:${inputRateSecByDuration} (${qps(inputRateSecByDuration)}万/秒)
                 |stage avgStage:              ${averageStageQps} (${qps(averageStageQps)}万/秒)
                 |--------------------------------------------------------------------------------------
                 |""".stripMargin
            println(str)
          }else{
            val str =
              s"""
                 |stage listenedDuration:      ${listenedDuration}毫秒
                 |stage duration:              ${duration}   stage耗时分析: RunTime:${metrics.executorRunTime}, DeserTime:${metrics.executorDeserializeTime}, DeserCpuTime:${metrics.executorDeserializeCpuTime}, resultSerTime:${metrics.resultSerializationTime}, jvmGCTime:${metrics.jvmGCTime}
                 |stage stageInputSize:        ${stageInputSize}
                 |stage listenerStageQps:${listenerStageQps}-- ${qps(listenerStageQps)}万/秒
                 |--------------------------------------------------------------------------------------
                 |""".stripMargin
            println(str)
          }
        }
        stageAttempts.remove(stageAttemptKey)
        if(stageAttempts.size()>10) stageAttempts.clear()
      }
    }

  }




  // 进行Job级别的监控
  val jobAttempts:ConcurrentHashMap[Int,Long] = new ConcurrentHashMap[Int,Long]()
  // 进行Stage级别的监控
  val jobQpsStat = CommonHelper.getInstance().collectHelper.createQueue(qpsEvictQueueSize)
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val startIme = jobStart.time
    val jobId = jobStart.jobId
    jobAttempts.put(jobId,System.nanoTime())
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    if(null !=jobEnd && jobAttempts.containsKey(jobEnd.jobId)){
      batchCounter.incrementAndGet()
      val startTime = jobAttempts.get(jobEnd.jobId)
      jobAttempts.remove(jobEnd.jobId)
      val listenedDuration = (System.nanoTime() - startTime)/1000000.00
      if(listenedDuration>0){
        jobEnd.jobResult match {
          case JobSucceeded =>{
            val listenedRate:Double= CommonHelper.divideRateAsSecond(recordSizePerJob,listenedDuration,2)
            var avgJobQps = 0.0
            if(batchCounter.get()> avgStartBatch){
              jobQpsStat.add(listenedRate)
              var sum = CommonHelper.getInstance().collectHelper.calculateQueueSum(jobQpsStat)
              avgJobQps = qps(sum,jobQpsStat.size())
            }
            val str =
              s"""
                 |Job listenedDuration:       ${listenedDuration}毫秒
                 |Job listenedRate            ${listenedRate}-- ${qps(listenedRate)}万/秒
                 |Job avgJobQps:              ${avgJobQps}--        ${qps(avgJobQps )}万/秒
                 |--------------------------------------------------------------------------------------
                 |""".stripMargin
            println(str)
          }
          case _ => None
        }
      }
    }
    if(jobAttempts.size()>10) jobAttempts.clear()
  }

  def qps(num:Double): Double ={
    BigDecimal(num)./(BigDecimal(10000)).setScale(2,RoundingMode.HALF_UP).doubleValue()
  }
  def qps(num:Double,b:Double): Double ={
    BigDecimal(num)./(BigDecimal(b)).setScale(2,RoundingMode.HALF_UP).doubleValue()
  }
}
