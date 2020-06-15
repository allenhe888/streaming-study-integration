package com.bigdata.streaming.spark.scala.streaming.kafkatest

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.bigdata.streaming.common.{CommonHelper, EvictingQueueImpl}
import com.bigdata.streaming.spark.scala.SSparkHelper
import org.apache.spark._
import org.apache.spark.scheduler._

import scala.math.BigDecimal.RoundingMode

class KafkaSparkListener(sparkConf:SparkConf) extends SparkListener {

  private val configRecordNumPerTask:Int=  sparkConf.get("spark.streaming.userdefine.config.record.num.per.task","100000").toInt
  private val partitionNum = sparkConf.get("spark.streaming.userdefine.partition.num","16").toInt
  private val configStageRecordNum:Int = configRecordNumPerTask * partitionNum

  private val avgBytesPerRecord = sparkConf.get("spark.streaming.userdefine.average.bytes.per.record","260").toInt

  private val configTaskRecordMiBytes:Double = configRecordNumPerTask * avgBytesPerRecord /1048576.0
  private val configStageRecordMiBytes:Double = configStageRecordNum * avgBytesPerRecord /1048576.0


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
  val taskQpsQueue = new EvictingQueueImpl[Double](qpsEvictQueueSize)
  val taskFlowRateQueue = new EvictingQueueImpl[Double](qpsEvictQueueSize)
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

      val inputRateSecByListener:Double= CommonHelper.divideRateAsSecond(configRecordNumPerTask,listenedDuration,2)
      val inputRateSecByDuration:Double= CommonHelper.divideRateAsSecond(configRecordNumPerTask,duration,2)
      var  averageTaskQps = 0.0
      if(batchCounter.get()>avgStartBatch){
        taskQpsQueue.add(inputRateSecByDuration)
        averageTaskQps = taskQpsQueue.tryAverage()
      }

      val metrics = taskEnd.taskMetrics

      val durationFlowRate= if(duration > 0){
        SSparkHelper.qpsAsSecond(configTaskRecordMiBytes ,duration)
      }else 0.0
      var  averageDurationFlowRate = 0.0
      if(batchCounter.get()>avgStartBatch){
        taskFlowRateQueue.add(durationFlowRate)
        averageDurationFlowRate = taskFlowRateQueue.tryAverage()
      }


      val str =
        s"""
           |task listenedDuration:      ${listenedDuration}
           |task duration:              ${duration}   耗时分析: RunTime:${metrics.executorRunTime}, DeserTime:${metrics.executorDeserializeTime}, DeserCpuTime:${metrics.executorDeserializeCpuTime}, resultSerTime:${metrics.resultSerializationTime}, jvmGCTime:${metrics.jvmGCTime}
           |task kafkaNum:              ${configRecordNumPerTask}
           |task inputRateSecByListener:${inputRateSecByListener}-- ${inputRateSecByListener/10000}万/秒
           |task inputRateSecByDuration:${inputRateSecByDuration}-- ${inputRateSecByDuration/10000}万/秒
           |task averageTaskQps:        ${averageTaskQps}--         ${averageTaskQps/10000}万/秒
           |task durationFlowRate:      ${durationFlowRate} Mi/sec            averageDurationFlowRate:${averageDurationFlowRate} Mi/sec
           |--------------------------------------------------------------------------------------
           |""".stripMargin
      println(str)

    }

  }


  // 进行Stage级别的监控
  val stageAttempts:ConcurrentHashMap[String,Long] = new ConcurrentHashMap[String,Long]()
  val stageQpsQueue = new EvictingQueueImpl[Double](qpsEvictQueueSize)
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
          val stageInputSize = configRecordNumPerTask * stageInfo.numTasks
          val listenerStageQps:Double= CommonHelper.divideRateAsSecond(stageInputSize,listenedDuration,2)
          val metrics = stageInfo.taskMetrics
          val duration = stageInfo.completionTime.get - stageInfo.submissionTime.get
          if(duration>0){
            val inputRateSecByDuration:Double= CommonHelper.divideRateAsSecond(stageInputSize,duration,2)
            var  averageStageQps = 0.0
            if(batchCounter.get()> avgStartBatch){
              stageQpsQueue.add(inputRateSecByDuration)
              averageStageQps = stageQpsQueue.tryAverage()
            }

            val stageFlowRate= if(duration > 0){
              SSparkHelper.qpsAsSecond(configStageRecordMiBytes ,duration)
            }else 0.0

            val str =
              s"""
                 |stage listenedDuration:      ${listenedDuration}毫秒
                 |stage duration:              ${duration}   stage耗时分析: RunTime:${metrics.executorRunTime}, DeserTime:${metrics.executorDeserializeTime}, DeserCpuTime:${metrics.executorDeserializeCpuTime}, resultSerTime:${metrics.resultSerializationTime}, jvmGCTime:${metrics.jvmGCTime}
                 |stage stageInputSize:        ${stageInputSize}
                 |stage listenerStageQps:      ${listenerStageQps}-- ${qps(listenerStageQps)}万/秒
                 |stage inputRateSecByDuration:${inputRateSecByDuration} (${qps(inputRateSecByDuration)}万/秒)
                 |stage avgStage:              ${averageStageQps} (${qps(averageStageQps)}万/秒)
                 |stage stageFlowRate:         ${stageFlowRate} Mi/sec
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
  val jobQpsStat = new EvictingQueueImpl[Double](qpsEvictQueueSize)
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
            val listenedRate:Double= CommonHelper.divideRateAsSecond(configStageRecordNum,listenedDuration,2)

            var avgJobQps = 0.0
            if(batchCounter.get()> avgStartBatch){
              jobQpsStat.add(listenedRate)
              avgJobQps = jobQpsStat.tryAverage()
            }

            val listenedFlowRate:Double= SSparkHelper.qpsAsSecond(configStageRecordMiBytes ,listenedDuration)

            val str =
              s"""
                 |Job listenedDuration:       ${listenedDuration}毫秒
                 |Job listenedRate            ${listenedRate}-- ${qps(listenedRate)}万/秒
                 |Job listenedFlowRate        ${listenedFlowRate} Mi/秒
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
