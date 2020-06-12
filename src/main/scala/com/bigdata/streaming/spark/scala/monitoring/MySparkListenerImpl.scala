package com.bigdata.streaming.spark.scala.monitoring

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import com.bigdata.streaming.common.CommonHelper
//import com.google.common.collect.EvictingQueue
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd, SparkListenerTaskStart, TaskInfo}

class MySparkListenerImpl(sparkConf:SparkConf) extends SparkListener with Logging{
  val sqlExecutor= CommonHelper
    .getJDBCHelper(3,"jdbc:mysql://ldsver51:3306/spark_monitor_db?characterEncoding=utf8","app","hq.2019.mysql")
    .startSqlExecutor()


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

  val taskAttemptStart:ConcurrentHashMap[String,Long] = new ConcurrentHashMap[String,Long]()


  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val info = taskStart.taskInfo
    val taskAttemptKey = Array(taskStart.stageId,taskStart.stageAttemptId,info.id,info.attemptNumber).mkString("_")
    taskAttemptStart.put(taskAttemptKey,System.currentTimeMillis())
  }


//  val distinctQueue:util.Queue[String] =  new ArrayBlockingQueue(1000)
//  def addIfNotExists(key:String):Boolean = {
//    if(distinctQueue.contains(key)){
//      false
//    }else{
//      distinctQueue.add(key)
//      true
//    }
//  }


  /* 创建 task_end_result表的Sql语句;

  CREATE TABLE `task_end_result` (
  `taskAttemptKey` varchar(100) NOT NULL,
  `taskId` varchar(100) DEFAULT NULL,
  `success` tinyint(1) DEFAULT NULL,
  `reasonMsg` varchar(200) DEFAULT NULL,
  `host` varchar(50) DEFAULT NULL,
  `executorId` varchar(50) DEFAULT NULL,
  `taskLocality` varchar(50) DEFAULT NULL,
  `stageId` varchar(50) DEFAULT NULL,
  `stageAttemptId` varchar(255) DEFAULT NULL,
  `taskType` tinyint(2) DEFAULT NULL,
  `launchTime` bigint(20) DEFAULT NULL,
  `finishTime` bigint(20) DEFAULT NULL,
  `duration` bigint(20) DEFAULT NULL,
  `listenedDuration` bigint(20) DEFAULT NULL,
  `executorDeserializeTime` bigint(20) DEFAULT NULL,
  `executorDeserializeCpuTime` bigint(20) DEFAULT NULL,
  `executorRunTime` bigint(20) DEFAULT NULL,
  `resultSerializationTime` bigint(20) DEFAULT NULL,
  `jvmGCTime` bigint(20) DEFAULT NULL,
  `resultSize` bigint(20) DEFAULT NULL,
  `memoryBytesSpilled` bigint(20) DEFAULT NULL,
  `diskBytesSpilled` bigint(20) DEFAULT NULL,
  `peakExecutionMemory` bigint(20) DEFAULT NULL,
  `bytesRead` bigint(20) DEFAULT NULL,
  `recordsRead` bigint(20) DEFAULT NULL,
  `inputRateSec` double(20,0) DEFAULT NULL,
  `bytesWritten` bigint(20) DEFAULT NULL,
  `recordsWritten` bigint(20) DEFAULT NULL,
  `outputRateSec` double(20,0) DEFAULT NULL,
  PRIMARY KEY (`taskAttemptKey`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


   */

  def onTaskEndImpl(taskEnd: SparkListenerTaskEnd): Unit ={
    val info = taskEnd.taskInfo
    if (info != null && taskEnd.stageAttemptId != -1) {
      //作业Fail,kill,发生异常,将发送预警信息
      val taskAttemptKey = Array(taskEnd.stageId,taskEnd.stageAttemptId,info.id,info.attemptNumber).mkString("_")
//      if(!addIfNotExists(taskAttemptKey)) return

      var connection = sqlExecutor.getConnection
      if(connection!=null){
        val currentMillis = System.currentTimeMillis()
        var success = false
        val reasonMsg: Option[String] = taskEnd.reason match {
          case Success =>
            success = true
            Some("Success")
          case kill: TaskKilled =>
            Some("TaskKilled:"+kill.toErrorString)
          case e: TaskFailedReason =>
            Some("TaskFailed: "+e.toErrorString)
          case _ =>
            Some("Task UnSucceed, Unkonw Reason")

        }

        val sql = "insert into task_end_result(taskAttemptKey,taskId,success,reasonMsg,host,executorId,taskLocality,stageId,stageAttemptId,taskType,launchTime,finishTime,duration,listenedDuration,executorDeserializeTime,executorDeserializeCpuTime,executorRunTime,resultSerializationTime,jvmGCTime,resultSize,memoryBytesSpilled,diskBytesSpilled,peakExecutionMemory,bytesRead,recordsRead,inputRateSec,bytesWritten,recordsWritten,outputRateSec)" +
          " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

        var ps = connection.prepareStatement(sql)
        var usedTime:Long = 2000
        val currentTime = System.currentTimeMillis()
        //      while(ps==null && usedTime > 0){
        //        ps = connection.prepareStatement(sql)
        //        usedTime = (System.currentTimeMillis()-currentTime)
        //        if(usedTime<=0){
        //          return
        //        }
        //      }

        //Stage相关信息
        val counter = new AtomicInteger(0)
        ps.setString(incrementAndGet(counter),taskAttemptKey)
        ps.setString(incrementAndGet(counter),info.id)
        ps.setBoolean(incrementAndGet(counter),success)
        ps.setString(incrementAndGet(counter),reasonMsg.get)

        ps.setString(incrementAndGet(counter),info.host)
        ps.setString(incrementAndGet(counter),info.executorId)
        ps.setString(incrementAndGet(counter),info.taskLocality.toString)
        ps.setInt(incrementAndGet(counter),taskEnd.stageId)
        ps.setInt(incrementAndGet(counter),taskEnd.stageAttemptId)
        ps.setInt(incrementAndGet(counter),getTaskTypeNum(taskEnd.taskType))

        // Task的运行时间相关
        ps.setLong(incrementAndGet(counter),info.launchTime)
        ps.setLong(incrementAndGet(counter),info.finishTime)
        ps.setLong(incrementAndGet(counter),info.duration)

        val taskStartTime = taskAttemptStart.get(taskAttemptKey)
        var listenedDuration:Long = 0L
        if(taskStartTime!=null){ //ListenedDuration
          listenedDuration = System.currentTimeMillis() - taskStartTime
        }else{
          listenedDuration = info.duration
        }
        ps.setLong(incrementAndGet(counter),listenedDuration)

        // Task执行细节度量: TaskMetrics
        val metrics = taskEnd.taskMetrics
        // Metrics - 用时记录
        ps.setLong(incrementAndGet(counter),metrics.executorDeserializeTime)
        ps.setLong(incrementAndGet(counter),metrics.executorDeserializeCpuTime)
        ps.setLong(incrementAndGet(counter),metrics.executorRunTime)
        ps.setLong(incrementAndGet(counter),metrics.resultSerializationTime)
        ps.setLong(incrementAndGet(counter),metrics.jvmGCTime)

        // Metrics - 内存监控
        ps.setLong(incrementAndGet(counter),metrics.resultSize)
        ps.setLong(incrementAndGet(counter),metrics.memoryBytesSpilled)
        ps.setLong(incrementAndGet(counter),metrics.diskBytesSpilled)
        ps.setLong(incrementAndGet(counter),metrics.peakExecutionMemory)

        // Metrics - Input/Output 监控
        //=================输入输出========================
        val inputMetrics = taskEnd.taskMetrics.inputMetrics
        val outputMetrics = taskEnd.taskMetrics.outputMetrics
        ps.setLong(incrementAndGet(counter),inputMetrics.bytesRead)
        ps.setLong(incrementAndGet(counter),inputMetrics.recordsRead)
        inputMetrics.recordsRead / (listenedDuration /1000.0)
        val inputRateSec:Double= CommonHelper.divideRateAsSecond(inputMetrics.recordsRead,listenedDuration,2)
        ps.setDouble(incrementAndGet(counter),inputRateSec) // inputRateSec

        ps.setLong(incrementAndGet(counter),outputMetrics.bytesWritten)
        ps.setLong(incrementAndGet(counter),outputMetrics.recordsWritten)
        val outputRateSec:Double= CommonHelper.divideRateAsSecond(outputMetrics.recordsWritten,listenedDuration,2)
        ps.setDouble(incrementAndGet(counter),outputRateSec) //outputRateSec

        // Metrics -Shuffle监控
        var isShuffleTask:Boolean = if(getTaskTypeNum(taskEnd.taskType) == 1)  true else false
        isShuffleTask = false
        if(isShuffleTask){
          ps.setLong(15,metrics.peakExecutionMemory)
          val shuffleReadMetrics = metrics.shuffleReadMetrics
          ps.setLong(14,shuffleReadMetrics.remoteBlocksFetched)
          ps.setLong(14,shuffleReadMetrics.localBlocksFetched)
          ps.setLong(14,shuffleReadMetrics.remoteBytesRead)
          ps.setLong(14,shuffleReadMetrics.localBytesRead)
          ps.setLong(14,shuffleReadMetrics.fetchWaitTime)
          ps.setLong(14,shuffleReadMetrics.recordsRead)

          val shuffleWriteMetrics = metrics.shuffleWriteMetrics
          ps.setLong(14,shuffleWriteMetrics.bytesWritten)
          ps.setLong(14,shuffleWriteMetrics.recordsWritten)
          ps.setLong(14,shuffleWriteMetrics.writeTime)

        }

        sqlExecutor.addSQLTask(ps,connection)

      }


    }

  }



  //任务结束的事件
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    try{
      onTaskEndImpl(taskEnd)
    }catch {
      case e:Exception => e.printStackTrace()
    }

  }



  //  val jedis = JedisUtil.getInstance().getJedis
  def onTaskEndImp0l(taskEnd: SparkListenerTaskEnd): Unit ={
    //在 onTaskEnd方法内可以获取的信息有
    /**
      * 1、taskMetrics
      * 2、shuffle
      * 3、task运行（input output）
      * 4、taskInfo
      **/

    val currentTimestamp = System.currentTimeMillis()
    // TaskMetrics（task的指标）可以拿到的指标
    /**
      * private val _executorDeserializeTime = new LongAccumulator
      * private val _executorDeserializeCpuTime = new LongAccumulator
      * private val _executorRunTime = new LongAccumulator
      * private val _executorCpuTime = new LongAccumulator
      * private val _resultSize = new LongAccumulator
      * private val _jvmGCTime = new LongAccumulator
      * private val _resultSerializationTime = new LongAccumulator
      * private val _memoryBytesSpilled = new LongAccumulator
      * private val _diskBytesSpilled = new LongAccumulator
      * private val _peakExecutionMemory = new LongAccumulator
      * private val _updatedBlockStatuses = new CollectionAccumulator[(BlockId, BlockStatus)]
      */
    val metrics = taskEnd.taskMetrics
    val taskMetricsMap = scala.collection.mutable.HashMap(
      "executorDeserializeTime" -> metrics.executorDeserializeTime, //executor的反序列化时间
      "executorDeserializeCpuTime" -> metrics.executorDeserializeCpuTime, //executor的反序列化的 cpu时间
      "executorRunTime" -> metrics.executorRunTime, //executoor的运行时间
      "resultSize" -> metrics.resultSize, //结果集大小
      "jvmGCTime" -> metrics.jvmGCTime, //
      "resultSerializationTime" -> metrics.resultSerializationTime,
      "memoryBytesSpilled" -> metrics.memoryBytesSpilled, //内存溢写的大小
      "diskBytesSpilled" -> metrics.diskBytesSpilled, //溢写到磁盘的大小
      "peakExecutionMemory" -> metrics.peakExecutionMemory //executor的最大内存
    )



    val jedisKey = "taskMetrics_" + {
      currentTimestamp
    }
    //    jedis.set(jedisKey, Json(DefaultFormats).write(jedisKey))
    //    jedis.pexpire(jedisKey, 3600)


    //======================shuffle指标================================
    val shuffleReadMetrics = metrics.shuffleReadMetrics
    val shuffleWriteMetrics = metrics.shuffleWriteMetrics

    //shuffleWriteMetrics shuffle读过程的指标有这些
    /**
      * private[executor] val _bytesWritten = new LongAccumulator
      * private[executor] val _recordsWritten = new LongAccumulator
      * private[executor] val _writeTime = new LongAccumulator
      */
    //shuffleReadMetrics shuffle写过程的指标有这些
    /**
      * private[executor] val _remoteBlocksFetched = new LongAccumulator
      * private[executor] val _localBlocksFetched = new LongAccumulator
      * private[executor] val _remoteBytesRead = new LongAccumulator
      * private[executor] val _localBytesRead = new LongAccumulator
      * private[executor] val _fetchWaitTime = new LongAccumulator
      * private[executor] val _recordsRead = new LongAccumulator
      */

    val shuffleMap = scala.collection.mutable.HashMap(
      "remoteBlocksFetched" -> shuffleReadMetrics.remoteBlocksFetched, //shuffle远程拉取数据块
      "localBlocksFetched" -> shuffleReadMetrics.localBlocksFetched, //本地块拉取
      "remoteBytesRead" -> shuffleReadMetrics.remoteBytesRead, //shuffle远程读取的字节数
      "localBytesRead" -> shuffleReadMetrics.localBytesRead, //读取本地数据的字节
      "fetchWaitTime" -> shuffleReadMetrics.fetchWaitTime, //拉取数据的等待时间
      "recordsRead" -> shuffleReadMetrics.recordsRead, //shuffle读取的记录总数
      "bytesWritten" -> shuffleWriteMetrics.bytesWritten, //shuffle写的总大小
      "recordsWritte" -> shuffleWriteMetrics.recordsWritten, //shuffle写的总记录数
      "writeTime" -> shuffleWriteMetrics.writeTime
    )

    val shuffleKey = s"shuffleKey${currentTimestamp}"
    //    jedis.set(shuffleKey, Json(DefaultFormats).write(shuffleMap))
    //    jedis.expire(shuffleKey, 3600)

    //=================输入输出========================
    val inputMetrics = taskEnd.taskMetrics.inputMetrics
    val outputMetrics = taskEnd.taskMetrics.outputMetrics

    val input_output = scala.collection.mutable.HashMap(
      "bytesRead" -> inputMetrics.bytesRead, //读取的大小
      "recordsRead" -> inputMetrics.recordsRead, //总记录数
      "bytesWritten" -> outputMetrics.bytesWritten,//输出的大小
      "recordsWritten" -> outputMetrics.recordsWritten//输出的记录数
    )
    val input_outputKey = s"input_outputKey${currentTimestamp}"
    //    jedis.set(input_outputKey, Json(DefaultFormats).write(input_output))
    //    jedis.expire(input_outputKey, 3600)



    //####################taskInfo#######
    val taskInfo: TaskInfo = taskEnd.taskInfo

    val taskInfoMap = scala.collection.mutable.HashMap(
      "taskId" -> taskInfo.taskId ,
      "host" -> taskInfo.host ,
      "speculative" -> taskInfo.speculative , //推测执行
      "failed" -> taskInfo.failed ,
      "killed" -> taskInfo.killed ,
      "running" -> taskInfo.running

    )


    val taskInfoKey = s"taskInfo${currentTimestamp}"
    //    jedis.set(taskInfoKey , Json(DefaultFormats).write(taskInfoMap))
    //    jedis.expire(taskInfoKey , 3600)

  }

  def onTaskEndImp02(taskEnd: SparkListenerTaskEnd): Unit = {

    if (taskEnd.taskInfo != null && taskEnd.stageAttemptId != -1){
      //作业Fail,kill,发生异常,将发送预警信息
      val errorMessage :Option[String]=
        taskEnd.reason match {
          case kill : TaskKilled=>
            Some(kill.toErrorString)
          case e : ExceptionFailure =>
            Some(e.toErrorString)
          case e: TaskFailedReason =>
            Some(e.toErrorString)
          case _ => None
        }

      //依据配置参数,可以动态的调整是否要开启作业的监控
      if (sparkConf.getBoolean("spark.sendEmail.OnTaskFail.enabled",true)){
        if (errorMessage.nonEmpty){
          println("------------------------发送预警邮件等等操作-------------------------")
        }

        println("------------------------监控到数据啦----保存数据到数据库等操作-----------------------------")

        val str =
          s"""
             |task任务名称:--------------${sparkConf.get("spark.app.name")}
             |task任务stageid:-----------${taskEnd.stageId}
             |task任务stageAttemptId:----${taskEnd.stageAttemptId}
             |task任务状态:---------------${taskEnd.reason}
             |task任务taskType:-----------${taskEnd.taskType}
             |----------------------------------taskInfo信息---------------------------------
             |
             |task任务excutorid:-------${taskEnd.taskInfo.executorId}
             |task任务是否失败:---------${taskEnd.taskInfo.failed}
             |task任务运行的所在host:---------${taskEnd.taskInfo.host}
             |task任务运行finishTime:---------${taskEnd.taskInfo.finishTime}
             |----------------------------------taskMetrics-task指标-------------------------------------
             |
             |task任务数据resultSize:---------${taskEnd.taskMetrics.resultSize}
             |task任务excutor结果集序列化所用时间:---------${taskEnd.taskMetrics.resultSerializationTime}
             |task任务数据executorCpuTime:---------${taskEnd.taskMetrics.executorCpuTime}
             |task任务excutor运行时间:---------${taskEnd.taskMetrics.executorRunTime}
             |task任务excutor反序列化所用cpu时间:---------${taskEnd.taskMetrics.executorDeserializeCpuTime}
             |task任务excutor反序列化所用时间:---------${taskEnd.taskMetrics.executorDeserializeTime}
             |task任务excutor数据溢写到磁盘的大小:---------${taskEnd.taskMetrics.diskBytesSpilled}
             |-----The number of in-memory bytes spilled by this task.
             |task任务excutor内存溢写的大小:---------${taskEnd.taskMetrics.memoryBytesSpilled}
             |task任务excutor内存大小:---------${taskEnd.taskMetrics.peakExecutionMemory}
             |--可以通过配置,关闭,减少内存的使用(跟踪block状态,会占用过多的内存使用)
             |task任务修改的block状态信息集合:---------${taskEnd.taskMetrics.updatedBlockStatuses}
             |task任务JVM GC耗时:---------${taskEnd.taskMetrics.jvmGCTime}
             |
             |----------------------------------taskMetrics-task指标outputMetrics/inputMetrics-------------------------------------
             |task任务读取的数据量条数:---------${taskEnd.taskMetrics.inputMetrics.recordsRead}
             |task任务读取数据量大小:---------${taskEnd.taskMetrics.inputMetrics.bytesRead}
             |task任务输出数据条数:---------${taskEnd.taskMetrics.outputMetrics.recordsWritten}
             |task任务输出数据量大小:---------${taskEnd.taskMetrics.outputMetrics.bytesWritten}
             |
             |-------------------------------------taskMetrics-task-shuffle指标--------------------------------------
             |-----------------------------shuffleReadMetrics---------------------
             |task任务shuffle读取数据的条数:---------${taskEnd.taskMetrics.shuffleReadMetrics.recordsRead}
             |task任务拉取数据的等待时间:---------${taskEnd.taskMetrics.shuffleReadMetrics.fetchWaitTime}
             |task任务本地块拉取的数量:---------${taskEnd.taskMetrics.shuffleReadMetrics.localBlocksFetched}
             |task任务读取本地数据的大小字节:---------${taskEnd.taskMetrics.shuffleReadMetrics.localBytesRead}
             |task任务远程拉取数据块数量:---------${taskEnd.taskMetrics.shuffleReadMetrics.remoteBlocksFetched}
             |task任务远程读取数据的字节数:---------${taskEnd.taskMetrics.shuffleReadMetrics.remoteBytesRead}

             |task任务shuffle拉取块总数:---------${taskEnd.taskMetrics.shuffleReadMetrics.totalBlocksFetched}
             |task任务shuffle读取数据总字节大小:---------${taskEnd.taskMetrics.shuffleReadMetrics.totalBytesRead}
             |--------------------------------shuffleWriteMetrics-------------------
             |task任务shuffle写到磁盘或内存所用时间:---------${taskEnd.taskMetrics.shuffleWriteMetrics.writeTime}
             |task任务shuffle写数据字节大小:---------${taskEnd.taskMetrics.shuffleWriteMetrics.bytesWritten}
             |task任务shuffle写数据条数:---------${taskEnd.taskMetrics.shuffleWriteMetrics.recordsWritten}
             |
             |""".stripMargin

        println(str)

      }


    }

  }


}
