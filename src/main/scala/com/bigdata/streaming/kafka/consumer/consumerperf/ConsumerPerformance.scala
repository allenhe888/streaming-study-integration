package com.bigdata.streaming.kafka.consumer.consumerperf

import java.nio.channels.ClosedByInterruptException
import java.text.SimpleDateFormat
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.{Collections, Properties, Random}

import com.bigdata.streaming.kafka.common.KafkaDevHelper
import com.bigdata.streaming.kafka.common.KafkaDevHelper.ConsumerHelp
import com.bigdata.streaming.kafka.consumer.PerfConfig
import com.bigdata.streaming.kafka.consumer.commhelper.RebalanceListenerImpl
import kafka.consumer.{Consumer, ConsumerConnector, ConsumerTimeoutException, KafkaStream}
import kafka.utils.{CommandLineUtils, ToolsUtils}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{Metric, MetricName}
import org.apache.log4j.Logger

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Performance test for the full zookeeper consumer
 */
object ConsumerPerformance {
  private val logger = Logger.getLogger(getClass())

  def main(args: Array[String]): Unit = {

    val config = new ConsumerPerfConfig(args)
    logger.info("Starting consumer...")
    val totalMessagesRead = new AtomicLong(0)
    val totalBytesRead = new AtomicLong(0)
    val consumerTimeout = new AtomicBoolean(false)
    var metrics: mutable.Map[MetricName, _ <: Metric] = null

    if (!config.hideHeader) {
      if (!config.showDetailedStats)
        println("start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec")
//      else
//        println("time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec")
    }

    var startMs, endMs = 0L
    if (!config.useOldConsumer) {
      val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config.props)
      consumer.subscribe(Collections.singletonList(config.topic))
      startMs = System.currentTimeMillis
      consume(consumer, List(config.topic), config.numMessages, 5000, config, totalMessagesRead, totalBytesRead)
      endMs = System.currentTimeMillis

      if (config.printMetrics) {
        metrics = consumer.metrics().asScala
      }
      ConsumerHelp.queryLatestOffsetsForAssignedTPs(consumer,null,true);
      consumer.close()
    } else {
      import kafka.consumer.ConsumerConfig
      val consumerConfig = new ConsumerConfig(config.props)
      val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
      val topicMessageStreams = consumerConnector.createMessageStreams(Map(config.topic -> config.numThreads))
      var threadList = List[ConsumerPerfThread]()
      for (streamList <- topicMessageStreams.values)
        for (i <- 0 until streamList.length)
          threadList ::= new ConsumerPerfThread(i, "kafka-zk-consumer-" + i, streamList(i), config, totalMessagesRead, totalBytesRead, consumerTimeout)

      logger.info("Sleeping for 1 second.")
      Thread.sleep(1000)
      logger.info("starting threads")
      startMs = System.currentTimeMillis
      for (thread <- threadList)
        thread.start
      for (thread <- threadList)
        thread.join
      endMs =
        if (consumerTimeout.get()) System.currentTimeMillis - consumerConfig.consumerTimeoutMs
        else System.currentTimeMillis
      consumerConnector.shutdown()
    }
    val elapsedSecs = (endMs - startMs) / 1000.0
      val totalMBRead = (totalBytesRead.get * 1.0) / (1024 * 1024)
      KafkaDevHelper.printQPSPerBatch(totalMessagesRead.get,(endMs - startMs));
//      println("%s, %s, %.4f, %.4f, \tin.nMsg=%d, \tnMsg.sec=%.4f \n\n".format(config.dateFormat.format(startMs), config.dateFormat.format(endMs),
//        totalMBRead, totalMBRead / elapsedSecs, totalMessagesRead.get, totalMessagesRead.get / elapsedSecs))

    if (metrics != null) {
      ToolsUtils.printMetrics(metrics)
    }

  }

  def consume(consumer: KafkaConsumer[Array[Byte], Array[Byte]], topics: List[String], count: Long, timeout: Long, config: ConsumerPerfConfig, totalMessagesRead: AtomicLong, totalBytesRead: AtomicLong) {
    var bytesRead = 0L
    var messagesRead = 0L
    var lastBytesRead = 0L
    var lastMessagesRead = 0L

    // Wait for group join, metadata fetch, etc
    val joinTimeout = 10000
    val isAssigned = new AtomicBoolean(false)
    val rebalanceCounter = new AtomicLong(0)
    consumer.subscribe(topics.asJava, new RebalanceListenerImpl(rebalanceCounter,isAssigned))
    val joinStart = System.currentTimeMillis()
    while (!isAssigned.get()) {
      if (System.currentTimeMillis() - joinStart >= joinTimeout) {
        throw new Exception("Timed out waiting for initial group join.")
      }
      consumer.poll(100)
    }

    val assignment = ConsumerHelp.doAssignTP(consumer,100)
    ConsumerHelp.seekToANewPosition(consumer, assignment, 0)
//    consumer.seekToBeginning(Collections.emptyList())

    // Now start the benchmark
    val startMs = System.currentTimeMillis
    var lastReportTime: Long = startMs
    var lastConsumedTime = System.currentTimeMillis
    var currentTimeMillis = lastConsumedTime
    ConsumerHelp.seekToANewPosition(consumer, assignment, 0)
    while (messagesRead < count && currentTimeMillis - lastConsumedTime <= timeout) {
      val records = consumer.poll(500).asScala
      currentTimeMillis = System.currentTimeMillis
      if (records.nonEmpty){
        lastConsumedTime = currentTimeMillis
      }else{
        println("空batch")
      }

      for (record <- records) {
        messagesRead += 1
        if (record.key != null)
          bytesRead += record.key.size
        if (record.value != null)
          bytesRead += record.value.size

        if (currentTimeMillis - lastReportTime >= config.reportingInterval) {
          if (config.showDetailedStats)
            printProgressMessage(0, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, lastReportTime, currentTimeMillis, config.dateFormat)
          lastReportTime = currentTimeMillis
          lastMessagesRead = messagesRead
          lastBytesRead = bytesRead
        }
      }
    }



    totalMessagesRead.set(messagesRead)
    totalBytesRead.set(bytesRead)
  }

  def printProgressMessage(id: Int, bytesRead: Long, lastBytesRead: Long, messagesRead: Long, lastMessagesRead: Long,
    startMs: Long, endMs: Long, dateFormat: SimpleDateFormat) = {
    val elapsedMs: Double = endMs - startMs
    val totalMBRead = (bytesRead * 1.0) / (1024 * 1024)
    val mbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024)

//    println("QPS="+(( messagesRead-lastMessagesRead) /elapsedMs*10.0)+" ,\t MB.nMsg="+1000.0 * (mbRead / elapsedMs))
    printf("\tQPS= %.3f 万/sec, \t MB.nMsg=%.3f \n",(( messagesRead-lastMessagesRead) /elapsedMs) /10,1000.0 * (mbRead / elapsedMs))
//    println("%s, %d, %.4f, %.4f, %d, %.4f".format(dateFormat.format(endMs), id, totalMBRead,
//      1000.0 * (mbRead / elapsedMs), messagesRead, ((messagesRead - lastMessagesRead) / elapsedMs) * 1000.0))

  }

  class ConsumerPerfConfig(args: Array[String]) extends PerfConfig(args) {
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED (only when using old consumer): The connection string for the zookeeper connection in the form host:port. " +
      "Multiple URLS can be given to allow fail-over. This option is only used with the old consumer.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])
    val bootstrapServersOpt = parser.accepts("broker-list", "REQUIRED (unless old consumer is used): A broker list to use for connecting if using the new consumer.")
      .withRequiredArg()
      .describedAs("host")
      .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val groupIdOpt = parser.accepts("group", "The group id to consume on.")
      .withRequiredArg
      .describedAs("gid")
      .defaultsTo("perf-consumer-" + new Random().nextInt(100000))
      .ofType(classOf[String])
    val fetchSizeOpt = parser.accepts("fetch-size", "The amount of data to fetch in a single request.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1024 * 1024)
    val resetBeginningOffsetOpt = parser.accepts("from-latest", "If the consumer does not already have an established " +
      "offset to consume from, start with the latest message present in the log rather than the earliest message.")
    val socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(2 * 1024 * 1024)
    val numThreadsOpt = parser.accepts("threads", "Number of processing threads.")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(10)
    val numFetchersOpt = parser.accepts("num-fetch-threads", "Number of fetcher threads.")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)
    val newConsumerOpt = parser.accepts("new-consumer", "Use the new consumer implementation. This is the default.")
    val consumerConfigOpt = parser.accepts("consumer.config", "Consumer config properties file.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])
    val printMetricsOpt = parser.accepts("print-metrics", "Print out the metrics. This only applies to new consumer.")

    val options = parser.parse(args: _*)

    CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, numMessagesOpt)

    val useOldConsumer = options.has(zkConnectOpt)
    val printMetrics = options.has(printMetricsOpt)

    val props = if (options.has(consumerConfigOpt))
      Utils.loadProps(options.valueOf(consumerConfigOpt))
    else
      new Properties
    if (!useOldConsumer) {
      CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServersOpt)
      import org.apache.kafka.clients.consumer.ConsumerConfig
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServersOpt))
      props.put(ConsumerConfig.GROUP_ID_CONFIG, options.valueOf(groupIdOpt))
      props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, options.valueOf(socketBufferSizeOpt).toString)
      props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, options.valueOf(fetchSizeOpt).toString)
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, if (options.has(resetBeginningOffsetOpt)) "latest" else "earliest")
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer])
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer])
      props.put(ConsumerConfig.CHECK_CRCS_CONFIG, "false")
    } else {
      if (options.has(bootstrapServersOpt))
        CommandLineUtils.printUsageAndDie(parser, s"Option $bootstrapServersOpt is not valid with $zkConnectOpt.")
      else if (options.has(newConsumerOpt))
        CommandLineUtils.printUsageAndDie(parser, s"Option $newConsumerOpt is not valid with $zkConnectOpt.")
      CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt, numMessagesOpt)
      props.put("group.id", options.valueOf(groupIdOpt))
      props.put("socket.receive.buffer.bytes", options.valueOf(socketBufferSizeOpt).toString)
      props.put("fetch.message.max.bytes", options.valueOf(fetchSizeOpt).toString)
      props.put("auto.offset.reset", if (options.has(resetBeginningOffsetOpt)) "largest" else "smallest")
      props.put("zookeeper.connect", options.valueOf(zkConnectOpt))
      props.put("consumer.timeout.ms", "1000")
      props.put("num.consumer.fetchers", options.valueOf(numFetchersOpt).toString)
    }
    val numThreads = options.valueOf(numThreadsOpt).intValue
    val topic = options.valueOf(topicOpt)
    val numMessages = options.valueOf(numMessagesOpt).longValue
    val reportingInterval = options.valueOf(reportingIntervalOpt).intValue
    if (reportingInterval <= 0)
      throw new IllegalArgumentException("Reporting interval must be greater than 0.")
    val showDetailedStats = options.has(showDetailedStatsOpt)
    val dateFormat = new SimpleDateFormat(options.valueOf(dateFormatOpt))
    val hideHeader = options.has(hideHeaderOpt)
  }

  class ConsumerPerfThread(threadId: Int, name: String, stream: KafkaStream[Array[Byte], Array[Byte]],
    config: ConsumerPerfConfig, totalMessagesRead: AtomicLong, totalBytesRead: AtomicLong, consumerTimeout: AtomicBoolean)
    extends Thread(name) {

    override def run() {
      var bytesRead = 0L
      var messagesRead = 0L
      val startMs = System.currentTimeMillis
      var lastReportTime: Long = startMs
      var lastBytesRead = 0L
      var lastMessagesRead = 0L

      try {
        val iter = stream.iterator
        while (iter.hasNext && messagesRead < config.numMessages) {
          val messageAndMetadata = iter.next
          messagesRead += 1
          bytesRead += messageAndMetadata.message.length
          val currentTimeMillis = System.currentTimeMillis

          if (currentTimeMillis - lastReportTime >= config.reportingInterval) {
            if (config.showDetailedStats)
              printProgressMessage(threadId, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, lastReportTime, currentTimeMillis, config.dateFormat)
            lastReportTime = currentTimeMillis
            lastMessagesRead = messagesRead
            lastBytesRead = bytesRead
          }
        }
      } catch {
        case _: InterruptedException =>
        case _: ClosedByInterruptException =>
        case _: ConsumerTimeoutException =>
          consumerTimeout.set(true)
        case e: Throwable => e.printStackTrace()
      }
      totalMessagesRead.addAndGet(messagesRead)
      totalBytesRead.addAndGet(bytesRead)
      if (config.showDetailedStats)
        printProgressMessage(threadId, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, startMs, System.currentTimeMillis, config.dateFormat)
    }

  }
}
