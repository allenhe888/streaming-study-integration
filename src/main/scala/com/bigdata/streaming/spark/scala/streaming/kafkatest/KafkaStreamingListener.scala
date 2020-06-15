package com.bigdata.streaming.spark.scala.streaming.kafkatest

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import com.bigdata.streaming.common.{CommonHelper, EvictingQueueImpl}
import com.bigdata.streaming.spark.scala.SSparkHelper
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted, StreamingListenerBatchStarted, StreamingListenerBatchSubmitted}

import scala.math.BigDecimal.RoundingMode

class KafkaStreamingListener(ssc: StreamingContext) extends StreamingListener{

  private val qpsEvictQueueSize = ssc.sparkContext.getConf.get("spark.streaming.userdefine.qps.evict.queue.size","100").toInt

  private val sparkConf = ssc.sparkContext.getConf
  private val configRecordNumPerTask:Int=  sparkConf.get("spark.streaming.userdefine.config.record.num.per.task","100000").toInt
  private val partitionNum = sparkConf.get("spark.streaming.userdefine.partition.num","16").toInt
  private val configStageRecordNum:Int = configRecordNumPerTask * partitionNum
  private val avgBytesPerRecord = sparkConf.get("spark.streaming.userdefine.average.bytes.per.record","260").toInt
  private val configStageRecordMiBytes:Double = configStageRecordNum * avgBytesPerRecord /1048576.0


  private val avgStartBatch = ssc.sparkContext.getConf.get("spark.streaming.userdefine.avg.start.batch","20").toInt
  // 先提交 "JobScheduler"线程: case JobStarted(job, startTime) => handleJobStart(job, startTime)
  /**
    * "RecurringTimer - JobGenerator"线程: 定时callback() => eventLoop.post(GenerateJobs(new Time(longTime)))
    *   -> "JobGenerator"线程中 : JobGenerator.processEvent():case GenerateJobs: => generateJobs(time) 来生成各RDDs,并令其线程提交执行:
    *       - val jobs =graph.generateJobs(time) => outputStreams.flatMap(outputStream => {outputStream.generateJob(time)}
    *       - jobs jobs match{case Success(jobs) => jobScheduler.submitJobSet(JobSet())}
    *           - listenerBus.post(StreamingListenerBatchSubmitted(jobSet.toBatchInfo))
    *               ->   1. 触发这里 onBatchSubmitted() 提交方法;
    *
    *           - jobs.foreach(job => jobExecutor.execute(new JobHandler(job))) : 另其线程,完成整个Batch的 start和completed;
    *             -> 进入"streaming-job-executor-n"线程: JobHandler.run():
    *                *  _eventLoop.post(JobStarted(job))
    *                   -> 进入"JobScheduler"线程: JobScheduler.processEvent(event): case JobStarted(job, startTime) => handleJobStart(job, startTime)
    *                     -> ifFirst(): listenerBus.post(StreamingListenerBatchStarted(jobSet.toBatchInfo)) 进入监听器的onStart()
    *                       -> 2. 进入这里的 onBatchStarted()方法;
    *
    *                * job.run(): 将这个jobSet提交到DAGScheduler
    *                   -> func() > body() -> ForEachDStream.foreachFunc(rdd, time) => dstream.foreachRDD(rdd) => rdd.count() => sc.runJob()
    *
    *                * _eventLoop.post(JobCompleted(job, clock.getTimeMillis())) => 发送JobCompleted消息给"JobScheduler"线程;
    *                   -> 进入"JobScheduler"线程: JobScheduler.processEvent(event): case JobCompleted(job, completedTime) => handleJobCompletion(job, completedTime)
    *                     - if (jobSet.hasCompleted): listenerBus.post(StreamingListenerBatchCompleted(jobSet.toBatchInfo)) 当jobs中最后一个Job也计算完, 就触发 这里的onBatchCompleted()
    *                       -> 3. 进入这里 onBatchCompleted()方法:
    */

  val batchSubmitTimes:ConcurrentHashMap[Long,Long] = new ConcurrentHashMap[Long,Long]()
  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    if(null !=batchSubmitted && null != batchSubmitted.batchInfo.batchTime ){
      batchSubmitTimes.put(batchSubmitted.batchInfo.batchTime.milliseconds,System.nanoTime())
    }
  }


  val batchStartTimes:ConcurrentHashMap[Long,Long] = new ConcurrentHashMap[Long,Long]()
  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    if(null !=batchStarted && null != batchStarted.batchInfo.batchTime ){
      batchStartTimes.put(batchStarted.batchInfo.batchTime.milliseconds,System.nanoTime())
    }
  }


  val batchCounter = new AtomicLong(0)
  val batchQpsStat = new EvictingQueueImpl[Double](qpsEvictQueueSize)
  val batchFlowRateStat = new EvictingQueueImpl[Double](qpsEvictQueueSize)
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    if(null !=batchCompleted && null != batchCompleted.batchInfo.batchTime && batchSubmitTimes.containsKey(batchCompleted.batchInfo.batchTime.milliseconds) ){
      val batchInfo = batchCompleted.batchInfo
      val inputRecordNum = batchInfo.numRecords
      // 计算监听器所监控到的性能表现:
      val listenerSubmit = batchSubmitTimes.get(batchInfo.batchTime.milliseconds) //最早, 在"JobGenerator"线程jobScheduler.submitJobSet()方法中,将生成的jobs依次提交给jobScheduler;
      batchSubmitTimes.remove(batchInfo.batchTime.milliseconds)
      val listenerStart = batchStartTimes.get(batchInfo.batchTime.milliseconds) // 其次, 在"streaming-job-executor-n"线程中,多个jobs中的第一个job,在job.run()前进入这里;
      batchStartTimes.remove(batchInfo.batchTime.milliseconds)
      val listenerCompleted = System.nanoTime() // 最后,当"streaming-job-executor-n"线程中job.run()完毕,发JobCompleted给"JobScheduler"线程,发现jobs中最后一个Job也计算完毕,就进入这里;
      val listenerElapsed = (listenerCompleted - listenerStart)/1000000.00
      val listenerQPS= if(listenerElapsed > 0){
          CommonHelper.divideRateAsSecond(inputRecordNum,listenerElapsed,2)
        }else{
          0
        }
      val submitToStart = (listenerStart - listenerSubmit)/1000000.00  //从生成jobs后提交开始, 到第一个job开始job.run();这启动的submitToStart()时间;
      val submitToCompleted = (listenerCompleted - listenerSubmit)/1000000.00  //从生成jobs后提交开始, 到第一个job开始job.run();这启动的submitToStart()时间;

      val listenerFlowRate= if(listenerElapsed > 0){
        SSparkHelper.qpsAsSecond(configStageRecordMiBytes,listenerElapsed)
      }else 0.0


      val submissionTime = batchInfo.submissionTime
      val processStart = batchInfo.processingStartTime.get
      val processEnd = batchInfo.processingEndTime.get
      val processingElapsed = batchInfo.processingDelay.get
      val schedulingElapsed = batchInfo.schedulingDelay.get
      val totalElapsed = batchInfo.totalDelay.get
      val processingQPS= if(processingElapsed > 0){
          CommonHelper.divideRateAsSecond(inputRecordNum,processingElapsed,2)
        }else{
          0
        }

      var avgBatchQPS = 0.0
      if(batchCounter.get()> avgStartBatch){
        batchQpsStat.add(listenerQPS)
        avgBatchQPS = batchQpsStat.tryAverage()
      }


      val processingFlowRate= if(listenerElapsed > 0){
          SSparkHelper.qpsAsSecond(configStageRecordMiBytes,processingElapsed)
        }else 0.0
      var avgProcessFlowRate = 0.0
      if(batchCounter.get()> avgStartBatch){
        batchFlowRateStat.add(processingFlowRate)
        avgProcessFlowRate = batchFlowRateStat.tryAverage()
      }

      val str =
        s"""
           |Batch - ${batchCounter.incrementAndGet()}: 应于[ ${toDatetime(batchInfo.batchTime.milliseconds)}]时刻触发的Batch, inputRecordNum:${inputRecordNum}
           |Batch 监听器 listenerElapsed:        ${listenerElapsed} 毫秒                                       listenerQPS:${listenerQPS}  (${qps(listenerQPS)}万/秒)
           |Batch 监听器 submitToStart:          ${submitToStart}  毫秒:提交到Job1.run()                       submitToCompleted:${submitToCompleted} 毫秒:提交到所有Jobs run()完
           |Batch 监听器 avgBatchQPS:            ${avgBatchQPS} (${qps(avgBatchQPS)}万/秒)
           |Batch submissionTime:                ${toHours(submissionTime)}
           |Batch processStart:                  ${toHours(processStart)}
           |Batch processEnd:                    ${toHours(processEnd)}    processElapsed:耗时${processingElapsed}毫秒        processingQPS:${processingQPS}(${qps(processingQPS)}万/秒)
           |Batch totalElapsed:                  ${totalElapsed}           schedulingElapsed:${schedulingElapsed}毫秒的调度
           |Batch listenerFlowRate:              ${listenerFlowRate} Mi/sec
           |Batch processingFlowRate:            ${processingFlowRate} Mi/sec       avgProcessFlowRate:${avgProcessFlowRate} Mi/sec
           |--------------------------------------------------------------------------------------
           |""".stripMargin
      println(str)

    }
    if(batchStartTimes.size()>10) batchStartTimes.clear()
    if(batchSubmitTimes.size()>500) batchSubmitTimes.clear()
  }

  private val format = new SimpleDateFormat("HH:mm:ss.SSS")
  def toHours(timeMillis:Long):String={
    format.format(new Date(timeMillis))
  }
  private val datetimeFormat = new SimpleDateFormat("yyyy-MM-hh HH:mm:ss.SSS")
  def toDatetime(timeMillis:Long):String={
    datetimeFormat.format(new Date(timeMillis))
  }

  def qps(num:Double): Double ={
    BigDecimal(num)./(BigDecimal(10000)).setScale(2,RoundingMode.HALF_UP).doubleValue()
  }
  def qps(num:Double,b:Double): Double ={
    BigDecimal(num)./(BigDecimal(b)).setScale(2,RoundingMode.HALF_UP).doubleValue()
  }

}
