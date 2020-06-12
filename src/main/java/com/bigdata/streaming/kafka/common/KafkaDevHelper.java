package com.bigdata.streaming.kafka.common;

import com.bigdata.streaming.common.CommonHelper;
import com.bigdata.streaming.common.StaticCounter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import javafx.util.Pair;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaDevHelper extends CommonHelper {

    private static KafkaDevHelper instance;
    public static KafkaDevHelper getInstance(){
        if(instance==null) instance = new KafkaDevHelper();
        return instance;
    }


    public static void printByteBufferMessageSet(ByteBufferMessageSet messageAndOffsets) throws UnsupportedEncodingException {
        for(MessageAndOffset msg:messageAndOffsets){
            ByteBuffer payload = msg.message().payload();
            byte[] valueBytes = new byte[payload.limit()];
            payload.get(valueBytes);

            ByteBuffer key = msg.message().key();
            byte[] keyBytes = new byte[key.limit()];
            key.get(keyBytes);

            long offset = msg.offset();
            int size = msg.message().size();
            long timestamp = msg.message().timestamp();
            System.out.printf("\n Offset:%d , key=%s , payload=%s , size=%d , timestamp=%d \n",offset,
                    new String(keyBytes,"UTF-8"),
                    new String(valueBytes,"UTF-8"),
                    size,timestamp);
        }
    }

    public static String topic = "testKafkaApi";

    // consumer相关参数:
    public static String brokerList = "ldsver51:9092";
    public static String groupId = "ideaConsumerGroup";
    public static String clientId = "client-idea";
    public static String autoOffsetReset = "earliest";
    public static String enableAutoCommit = "false";

    public static String byteDeserializer = ByteArrayDeserializer.class.getName();
    public static String byteSerializer = ByteArraySerializer.class.getName();

    public static String bootstrapServers = "ldsver51:9092";
    public static String acls = "all";
    public static String stringDeserializer = StringDeserializer.class.getName();
    public static String stringSerializer = StringSerializer.class.getName();

    public static <K,V> KafkaConsumer<K,V> getKafkaConsumer(Map<String,Object> overrideProps) {
        Properties props = new Properties();
        props.put("key.deserializer", byteDeserializer);
        props.put("bootstrap.servers",brokerList);
        props.put("group.id", groupId);
//        props.put("client.id",clientId);
        props.put("auto.offset.reset",autoOffsetReset);
        props.put("enable.auto.commit",enableAutoCommit);
        props.put("receive.buffer.bytes","2097152");
        props.put("max.partition.fetch.bytes","1048576");
        props.put("value.deserializer", byteDeserializer);
//        props.put("session.timeout.ms", 50000);
        props.put("heartbeat.interval.ms", 5000);

        if(overrideProps!=null && !overrideProps.isEmpty())
            props.putAll(overrideProps);

        return new KafkaConsumer<K,V>(props);
    }


    public static void printQPS(double elapsedSec,long pollNum,List<Integer> batchRecords,boolean printBatch){
        System.out.printf("共读消费数据: %d (%.4f万) \t总用时; %.3f秒 \n",pollNum,pollNum/10000.0,elapsedSec);
        System.out.printf(" QPS统计: 每秒平均=%.3f. (%.4f 万/sec),\t 有效batch总数 %d \n",pollNum/elapsedSec, pollNum/(10000*elapsedSec),batchRecords !=null ? batchRecords.size():0);
    }

    public static void printQPSPerBatch(long thisBatchSize,double time){
        double elapsedSec = time/1000.0;
        System.out.printf("BatchSize=%d (%.4f万),\t  用时 %.3f 秒,\t  QPS=%.3f ( %.3f万/sec) \n",
                thisBatchSize,thisBatchSize/10000.0,
                elapsedSec,
                thisBatchSize/elapsedSec, thisBatchSize/(10000*elapsedSec)
        );
    }


    public static String getRecordString(){
        return getInstance().dataGenerator.getJsonRecord().toJSONString();
    }

    private static void printFixRateReport(int totalRecordSize, int lastReportRecordSize,long currentTime, long lastReportTime, long reportingInterval) {
        if(currentTime - lastReportTime >= reportingInterval){
            printQPSPerBatch(totalRecordSize - lastReportRecordSize,currentTime - lastReportTime);
            lastReportTime = currentTime;
            lastReportRecordSize = totalRecordSize;
        }
    }


    public static String[] parsedConsumerArags(String... args) {
        String brokerList= "ldsver51:9092";
        String topic= "testStringPerf";
        String group= "testGroup";
        String recordNum= "1000000";

        if(args.length>0){
            brokerList = args[0];
        }
        if(args.length>1){
            topic = args[1];
        }
        if(args.length>2){
            group = args[2];
        }
        if(args.length>3){
            recordNum = args[3];
        }

        String[] parsedArgs = {brokerList, topic, group, recordNum};
        return parsedArgs;
    }

    public static <K,V> void testConsumerRate(KafkaConsumer<K,V> consumer,int recordNum,long consumerTimeout, String... topic) {
        consumer.subscribe(Arrays.asList(topic));
        Set<TopicPartition> topicPartitions =ConsumerHelp.doAssignTP(consumer, 100);
        ConsumerHelp.seekToANewPosition(consumer,topicPartitions,880);

        ConsumerHelp.queryLatestOffsetsForAssignedTPs(consumer,topicPartitions,true);
        ConsumerHelp.doConsumerPerformance(consumer, recordNum, consumerTimeout, true, 1000);

        ConsumerHelp.closeConsumer(consumer);

    }

    public PerformanceHelper performanceHelper  = new PerformanceHelper();

    public class PerformanceHelper{

        public <K,V> void doSubscribeTypePerformanceTestAndClose(KafkaConsumer<K,V> consumer, int recordNum, long consumerTimeout, String... topic) {
            consumer.subscribe(Arrays.asList(topic));
            Set<TopicPartition> topicPartitions =ConsumerHelp.doAssignTP(consumer, 100);
            ConsumerHelp.seekToANewPosition(consumer,topicPartitions,0);

            doConsumerPerformanceNotClose(consumer, recordNum, consumerTimeout, true, 1000);
            ConsumerHelp.closeConsumer(consumer);
        }

        public <K,V> void loopPerformanceTestInSubscribeType(KafkaConsumer<K,V> consumer,String topic, int startOffset, int recordNum, long consumerTimeout, int batchIntervalSec,int reportIntervalSec) {
            consumer.subscribe(Arrays.asList(topic));
            try{
                while (true){
                    Set<TopicPartition> topicPartitions =ConsumerHelp.doAssignTP(consumer, 100);
                    ConsumerHelp.seekToANewPosition(consumer,topicPartitions,startOffset);
                    doConsumerPerformanceNotClose(consumer, recordNum, consumerTimeout, true, reportIntervalSec *1000);
                    try {
                        Thread.sleep(batchIntervalSec*1000);
                    } catch (InterruptedException e) {e.printStackTrace();}
                }
            }finally {
                ConsumerHelp.closeConsumer(consumer);
            }
        }

        public <K,V> void loopPerformanceTestInAssignType(KafkaConsumer<K,V> consumer, List<TopicPartition> tps, int startOffset, int recordNum, long consumerTimeout, int batchIntervalSec, int reportIntervalSec) {
            consumer.assign(tps);
            try{
                while (true){
                    Set<TopicPartition> topicPartitions =ConsumerHelp.doAssignTP(consumer, 100);
                    ConsumerHelp.seekToANewPosition(consumer,topicPartitions,startOffset);
                    doConsumerPerformanceByFixNumNotClose(consumer, recordNum, consumerTimeout, true, reportIntervalSec *1000);
                    try {
                        Thread.sleep(batchIntervalSec*1000);
                    } catch (InterruptedException e) {e.printStackTrace();}
                }
            }finally {
                ConsumerHelp.closeConsumer(consumer);
            }
        }

        public <K,V> void doConsumerPerformanceNotClose(KafkaConsumer<K,V> consumer, int numRecords, long timeout, boolean timeoutByEmptyBatch, int reportingInterval) {
            if(timeout<=0){
                timeout=5000;
            }

            long lastDataTime = System.currentTimeMillis();
            ConsumerRecords<K,V> poll=null;
            AtomicLong batchCounter = new AtomicLong(0);
            long currentTime = System.currentTimeMillis();
            // each batch stat
            long lastReportTime = currentTime;
            int lastReportRecordSize = 0;
            // all batch stat
            long batchStart =  currentTime;
            int totalSize = 0;
            List<Long> emptyBatch = new ArrayList<>();
            long lastNanoTime =  System.nanoTime();
            while (totalSize< numRecords && System.currentTimeMillis() - lastDataTime <= timeout){
                currentTime=System.currentTimeMillis();
                poll = consumer.poll(500);
                batchCounter.incrementAndGet();
                if(poll!=null && !poll.isEmpty()){
                    totalSize  += poll.count();
                    if(timeoutByEmptyBatch){
                        lastDataTime = System.currentTimeMillis();
                    }
                    if(currentTime - lastReportTime >= reportingInterval){

                        printQPSPerBatch(totalSize - lastReportRecordSize,currentTime - lastReportTime);
                        lastReportTime = currentTime;
                        lastReportRecordSize = totalSize;

                        long currentNanoTime =  System.nanoTime();
                        long duration = currentNanoTime - lastNanoTime;

                        List<Long> batchAddedList = StaticCounter.getInstance().getListThreadLocalValues();
                        AtomicLong sum = new AtomicLong(0);
                        batchAddedList.forEach(e->{sum.addAndGet(e);});
                        long fetchUsedTime = sum.get();
                        if(duration>0){
                            double percent = BigDecimal.valueOf(fetchUsedTime*100).divide(BigDecimal.valueOf(duration),2, RoundingMode.HALF_UP).doubleValue();
                            System.err.println("本批sendFetch:  总次数"+batchAddedList.size()+", 运行总用时: "+duration
                                    +", 其中fetch总用时: "+fetchUsedTime+", fetch用时占总比: "+percent+"%");
                        }
                        StaticCounter.getInstance().removeListThreadLocal();
                        StaticCounter.getInstance().createListThreadLocal();
                        lastNanoTime = currentNanoTime;
                    }
                }else {
//                    System.out.println("空batch");
                    emptyBatch.add(batchCounter.get());
                }
            }
            ConsumerHelp.printPollConsumerRecords(poll,batchCounter);
            printQPSPerBatch(totalSize ,System.currentTimeMillis() - batchStart);
            System.err.println("获取为空的batch包括: "+emptyBatch.toString()+"\n        ------- * 运行完毕, 本批共poll() "+batchCounter+"次 *---------- \n\n ");

        }


        public <K,V> void doConsumerPerformanceByFixNumNotClose(KafkaConsumer<K,V> consumer, int numRecords, long timeout, boolean timeoutByEmptyBatch, int reportingInterval) {
            if(timeout<=0){
                timeout=5000;
            }

            long lastDataTime = System.currentTimeMillis();
            ConsumerRecords<K,V> poll=null;
            AtomicLong batchCounter = new AtomicLong(0);
            long currentTime = System.currentTimeMillis();
            // each batch stat
            long lastReportTime = currentTime;
            int lastReportRecordSize = 0;
            // all batch stat
            long batchStart =  currentTime;
            int totalSize = 0;
            List<Long> emptyBatch = new ArrayList<>();


            do{
                int pollTimesPerBatch = 1000;
                int batchPollSize= 0;
                AtomicInteger batchEmtpyCounter = new AtomicInteger(0);
                long batchStartNano = System.nanoTime();
                long lastHasDataTime = System.currentTimeMillis();
                for (int i = 0; i < pollTimesPerBatch; i++) {
                    poll = consumer.poll(500);
                    if(poll!=null && !poll.isEmpty()){
                        batchPollSize  += poll.count();
                        lastHasDataTime = System.currentTimeMillis();;
                    }else {
                        if(System.currentTimeMillis() - lastHasDataTime > timeout){
                            break;
                        }
                    }
//                    consumer.commitAsync();
                }
                long batchDuration = System.nanoTime() - batchStartNano;
                ConsumerHelp.queryLatestOffsetsForAssignedTPs(consumer, null, true);
                printQPSPerBatch(batchPollSize,batchDuration/1000000);
                long fetchUsedTime = StaticCounter.getInstance().getThreadTimeValue();
                int fetchTimes = StaticCounter.getInstance().getThreadBatchValue();
                if(batchDuration>0){
                    double percent = BigDecimal.valueOf(fetchUsedTime*100).divide(BigDecimal.valueOf(batchDuration),2, RoundingMode.HALF_UP).doubleValue();
                    System.err.println("本批: 发送Fetch请求次数"+fetchTimes+", 运行总用时: "+batchDuration/1000000 +", 其中sendFetch用时: "+fetchUsedTime/1000000+", fetch用时占总比: "+percent+"%");
                }
                StaticCounter.getInstance().removeThreadTimeAndBatchCount();
                StaticCounter.getInstance().createThreadTimeAndBatchCount();

                totalSize+= batchPollSize;
            }while (totalSize< numRecords);




//            while (totalSize< numRecords && System.currentTimeMillis() - lastDataTime <= timeout){
//                currentTime=System.currentTimeMillis();
//                poll = consumer.poll(500);
//                batchCounter.incrementAndGet();
//                if(poll!=null && !poll.isEmpty()){
//                    totalSize  += poll.count();
//                    if(timeoutByEmptyBatch){
//                        lastDataTime = System.currentTimeMillis();
//                    }
//                    if(currentTime - lastReportTime >= reportingInterval){
//
//                        printQPSPerBatch(totalSize - lastReportRecordSize,currentTime - lastReportTime);
//                        lastReportTime = currentTime;
//                        lastReportRecordSize = totalSize;
//
//                        long currentNanoTime =  System.nanoTime();
//                        long duration = currentNanoTime - lastNanoTime;
//
//                        List<Long> batchAddedList = StaticCounter.getInstance().getListThreadLocalValues();
//                        AtomicLong sum = new AtomicLong(0);
//                        batchAddedList.forEach(e->{sum.addAndGet(e);});
//                        long fetchUsedTime = sum.get();
//                        if(duration>0){
//                            double percent = BigDecimal.valueOf(fetchUsedTime*100).divide(BigDecimal.valueOf(duration),2, RoundingMode.HALF_UP).doubleValue();
//                            System.err.println("本批sendFetch:  总次数"+batchAddedList.size()+", 运行总用时: "+duration
//                                    +", 其中fetch总用时: "+fetchUsedTime+", fetch用时占总比: "+percent+"%");
//                        }
//                        StaticCounter.getInstance().removeListThreadLocal();
//                        StaticCounter.getInstance().createListThreadLocal();
//                        lastNanoTime = currentNanoTime;
//                    }
//                }else {
////                    System.out.println("空batch");
//                    emptyBatch.add(batchCounter.get());
//                }
//            }
//            ConsumerHelp.printPollConsumerRecords(poll,batchCounter);
            printQPSPerBatch(totalSize ,System.currentTimeMillis() - batchStart);
//            System.err.println("获取为空的batch包括: "+emptyBatch.toString()+"\n        ------- * 运行完毕, 本批共poll() "+batchCounter+"次 *---------- \n\n ");

        }


    }


    public static class ConsumerHelp{

        public static <K,V> KafkaConsumer<K,V> getKafkaConsumer(Map<String,Object> overrideProps) {
            String commGroup = "commTestGroup";
            Properties props = new Properties();
            props.put("key.deserializer", stringDeserializer);
            props.put("bootstrap.servers",brokerList);
            props.put("group.id", commGroup);
            props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 3);
//        props.put("client.id",clientId);
            props.put("auto.offset.reset",autoOffsetReset);
            props.put("enable.auto.commit",enableAutoCommit);
            props.put("receive.buffer.bytes","2097152");
            props.put("max.partition.fetch.bytes","1048576");
            props.put("value.deserializer", stringDeserializer);
//        props.put("session.timeout.ms", 50000);
            props.put("heartbeat.interval.ms", 5000);
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");// 设置隔离级别

            if(overrideProps!=null && !overrideProps.isEmpty())
                props.putAll(overrideProps);

            return new KafkaConsumer<K,V>(props);
        }

        public static <K, V> KafkaConsumer<K, V> createReadCommittedConsumer(String consumerGroup, int maxPollRecords,Map<String,Object> overrideProps) {
            if(overrideProps==null){
                overrideProps = new HashMap<>();
            }
            overrideProps.put(ConsumerConfig.GROUP_ID_CONFIG,consumerGroup);
            overrideProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,maxPollRecords);
            overrideProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");
            overrideProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
            KafkaConsumer<K, V> kafkaConsumer = getKafkaConsumer(overrideProps);
            return kafkaConsumer;
        }


        public static <K,V> void closeConsumer(KafkaConsumer<K,V> consumer) {
            if(consumer!=null){
                try{
                    consumer.close();
                }catch (Exception e){
                    consumer=null;
                }
            }
        }


        public static <K, V> void printPollConsumerRecords(ConsumerRecords<K, V> poll,AtomicLong pollCount) {
            pollCount.incrementAndGet();
            String pollStr=null;
            if(poll!=null && !poll.isEmpty()){
                if(!poll.isEmpty()){
                    LinkedList<ConsumerRecord> batchRecords = new LinkedList<>();
                    for(ConsumerRecord<K, V> data: poll){
                        batchRecords.addLast(data);
                    }
                    ConsumerRecord first = batchRecords.peekFirst();
                    ConsumerRecord last = batchRecords.peekLast();
                    pollStr = new StringJoiner(",\t")
                            .add("partition="+first.partition())
                            .add("batchSize="+batchRecords.size())
                            .add("first="+first.offset())
                            .add("last="+last.offset())
                            .toString();
                }
            }

            System.out.println("printBatch:"+pollCount.get()+",\t poll="+pollStr);
        }

        public static <K,V> List<ConsumerRecord<K,V>> doConsumer(KafkaConsumer<K,V> consumer, int numRecords, long timeout, boolean timeoutByEmptyBatch) {
            if(timeout<=0){
                timeout=5000;
            }
            List<ConsumerRecord<K,V>> list = new ArrayList<>();
            long lastDataTime = System.currentTimeMillis();
            ConsumerRecords<K,V> poll;
            AtomicLong pollCount = new AtomicLong(0);
            while (list.size()< numRecords && System.currentTimeMillis() - lastDataTime <= timeout){
                poll = consumer.poll(100);
                if(poll!=null && !poll.isEmpty()){
                    for(ConsumerRecord<K,V> data:poll){
                        list.add(data);
                    }
                    if(timeoutByEmptyBatch){
                        lastDataTime = System.currentTimeMillis();
                    }
                }
                ConsumerHelp.printPollConsumerRecords(poll,pollCount);
            }
            return list;
        }

        public static <K,V> void doConsumerInFixRate(KafkaConsumer<K,V> consumer,boolean runInFixBatchOrNum, int targetNum, int sleepMsPerBatch) {
            long lastHasDataBatch = System.currentTimeMillis();
            ConsumerRecords<K,V> poll;
            AtomicLong batchCount = new AtomicLong(0);


            if(runInFixBatchOrNum){
                for (int i = 0; i < targetNum; i++) {
                    poll = consumer.poll(100);
                    ConsumerHelp.printPollConsumerRecords(poll,batchCount);
                    try {
                        Thread.sleep(sleepMsPerBatch);
                    } catch (InterruptedException e) {}
                }
            }else {
                lastHasDataBatch = System.currentTimeMillis();
                AtomicLong polledRecords = new AtomicLong(0);
                while (polledRecords.get() < targetNum && !isNowTimeout(lastHasDataBatch,20000)){
                    poll = consumer.poll(100);
                    if(poll!=null && !poll.isEmpty()){
                        polledRecords.set(polledRecords.get()+poll.count());
                        lastHasDataBatch = System.currentTimeMillis();
                    }
                    ConsumerHelp.printPollConsumerRecords(poll,batchCount);
                    try {
                        Thread.sleep(sleepMsPerBatch);
                    } catch (InterruptedException e) {}
                }

            }

        }

        private static boolean isNowTimeout(long lastHasDataBatch, int timeout) {
            if(System.currentTimeMillis() - lastHasDataBatch > timeout){
                System.err.println("发生超时了, lastHasDataBatch="+lastHasDataBatch+", timeout="+timeout);
                return true;
            }else {
                return false;
            }
        }


        public static <K,V> List<ConsumerRecord<K,V>> doConsumerPerformance(KafkaConsumer<K,V> consumer, int numRecords, long timeout, boolean timeoutByEmptyBatch,int reportingInterval) {
            if(timeout<=0){
                timeout=5000;
            }
            long start =  System.currentTimeMillis();
            long lastDataTime = System.currentTimeMillis();
            ConsumerRecords<K,V> poll=null;
            AtomicLong pollCount = new AtomicLong(0);
            long currentTime = System.currentTimeMillis();

            long lastReportTime = currentTime;
            int lastReportRecordSize = 0;
            int totalSize = 0;
            while (totalSize< numRecords && System.currentTimeMillis() - lastDataTime <= timeout){
                currentTime=System.currentTimeMillis();
                poll = consumer.poll(500);
                if(poll!=null && !poll.isEmpty()){
                    totalSize  += poll.count();
                    if(timeoutByEmptyBatch){
                        lastDataTime = System.currentTimeMillis();
                    }
                    if(currentTime - lastReportTime >= reportingInterval){
                        printQPSPerBatch(totalSize - lastReportRecordSize,currentTime - lastReportTime);
                        lastReportTime = currentTime;
                        lastReportRecordSize = totalSize;
                    }
                }else {
                    System.out.println("空batch");
                }

            }
            printQPSPerBatch(totalSize ,currentTime - start);
            ConsumerHelp.printPollConsumerRecords(poll,pollCount);
            ConsumerHelp.queryLatestOffsetsForAssignedTPs(consumer,null,true);
            consumer.close();
            return Collections.emptyList();
        }

        public static <K,V> Set<TopicPartition> doAssignTP(KafkaConsumer<K,V> consumer, int maxTryNum){
            Set<TopicPartition> assignment = new HashSet<>();
            AtomicLong assignCount = new AtomicLong(0);
            while (assignment.size()==0){
                if(assignCount.incrementAndGet() > maxTryNum){
                    throw new RuntimeException("doAssign 操作次数大于>"+maxTryNum);
                }
                consumer.poll(100);
                assignment= consumer.assignment();
            }

            return assignment;
        }

        public static void previewRunningForResetBeginningOffsets(String brokerList,String topic,String consumerGroup){
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
            }

            Properties props = new Properties();
            props.put("key.deserializer", byteDeserializer);
            props.put("bootstrap.servers",brokerList);
            props.put("group.id", consumerGroup);
            props.put("auto.offset.reset","earliest");
            props.put("enable.auto.commit",false);
            props.put("value.deserializer", byteDeserializer);
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);

            KafkaConsumer consumer = new KafkaConsumer(props);
            consumer.subscribe(Collections.singleton(topic));
            Set<TopicPartition> assignment = doAssignTP(consumer,100);



            final long targetPosition = 0;
            seekToANewPosition(consumer,assignment,targetPosition);

            consumer.close();

            if(consumer!=null){
                consumer= null;
            }

        }

        public static <K, V> void seekToANewPosition(KafkaConsumer<K, V> consumer, Set<TopicPartition> assignment, final long targetPosition) {
            Map<String, List<String>> oldPosition  = queryLatestOffsetsForAssignedTPs(consumer, assignment, false);
            for(TopicPartition tp:assignment){
                consumer.seek(tp,targetPosition);
                while (targetPosition!=consumer.position(tp)){
                    consumer.seek(tp,targetPosition);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {}
                }
            }
            Map<String, List<String>> newPositions  = queryLatestOffsetsForAssignedTPs(consumer, assignment, false);
            System.out.println("seek 重新定位fetch位置: "+"\n\t seek()前 : \t" +oldPosition.toString()+
                    "\n\t seek()之后: \t"+newPositions.toString());
        }


        public static <K, V> Map<String, List<String>> queryLatestOffsetsForAssignedTPs(KafkaConsumer<K, V> consumer, Set<TopicPartition> assignment, boolean toPrint) {
            if(null ==assignment || assignment.isEmpty()){
                assignment = doAssignTP(consumer,100);
            }

            Map<String, List<String>> topics = new HashMap<>();
            for(TopicPartition tp:assignment){
                long curOffset = consumer.position(tp);
                List<String> list = topics.getOrDefault(tp.topic(), new ArrayList<>());
                list.add("分区"+tp.partition()+":"+curOffset);
                topics.put(tp.topic(),list);
            }
            if(toPrint){
                System.out.println("打印当前offsets信息:");
                topics.forEach((k,v)->{
                    System.out.println("\t topic( "+k+" ): "+v.toString());
                });
            }

            return topics;
        }

        public static <K,V> Map<TopicPartition, OffsetAndMetadata> getConsumerOffsetPosition(KafkaConsumer<K,V> consumer) {
            Map<TopicPartition, OffsetAndMetadata> toCommitOffsets = new HashMap<>();
            consumer.assignment().forEach(tp->{
                tp.partition();
                toCommitOffsets.put(tp,new OffsetAndMetadata(consumer.position(tp),"提交时间:"+System.currentTimeMillis()));
            });
            return toCommitOffsets;
        }


        public static <K, V> void resetToCommittedPositions(KafkaConsumer<K, V> consumer) {
            consumer.assignment().forEach(tp->{
                long oldPosition = consumer.position(tp);
                // Get the last committed offset from the server: 从Server端获取最新提交的(该分区-group下)offset位置;
                OffsetAndMetadata committed = consumer.committed(tp);

                // 将内存中的拉去offset位置,更新为 last-committed 值; 即重置,以重新拉去;
                if(committed!=null){
                    consumer.seek(tp,committed.offset());
                }else{
                    consumer.seekToBeginning(Collections.singleton(tp));
                }
                System.out.println(tp+"发生一次offset重置: \t"+oldPosition+" -> "+consumer.position(tp));
            });
        }


    }


    public static class ProducerHelp{
        public static String servers = brokerList;
        public static String transactionalId = "txId-common-produce";
        public static String topic = "testKafkaApi";
        public static String byteDeserializer = ByteArrayDeserializer.class.getName();
        public static String byteSerializer = ByteArraySerializer.class.getName();

        public static String bootstrapServers = "ldsver51:9092";
        public static String acls = "all";
        public static String stringDeserializer = StringDeserializer.class.getName();
        public static String stringSerializer = StringSerializer.class.getName();

        public static <K,V> void testProducerRun(KafkaProducer<K, V> producer,String topic, int numRecords,List<Pair<K,V>> sampleData) {
            if(sampleData==null || sampleData.isEmpty()) throw new IllegalArgumentException("sampleData cannot be empty"+sampleData);
            StateView stateView = new StateView(numRecords, 3000);
            ProducerRecord<K,V> record;
            Random random = new Random();
            for(int i=0;i<numRecords;i++){
                Pair<K, V> kv = sampleData.get(random.nextInt(sampleData.size()));
                record = new ProducerRecord<K,V>(topic, kv.getKey(),kv.getValue());
                Callback callback = stateView.nextCompletion(System.currentTimeMillis(), kv.toString().getBytes().length, stateView);
                producer.send(record,callback);
            }
            producer.flush();
            producer.close();
            stateView.printTotal();
        }


        public static <K,V> KafkaProducer<K, V> getProducer(Map<String,Object> overrideProps){
            Properties props = new Properties();
            props.put("bootstrap.servers",bootstrapServers);
            props.put("acks",acls);
            props.put(ProducerConfig.RETRIES_CONFIG,3);

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSerializer);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, stringSerializer);

            if(overrideProps!=null && !overrideProps.isEmpty()){
                props.putAll(overrideProps);
            }

            return new KafkaProducer<>(props);
        }


        public static <K,V> KafkaProducer<K, V> getTransactionProducer(Map<String,Object> overrideProps){
            String txProducerClientId = "clientId-tx-producer-common";
            String commTransactionId = "transactionId-common";
            Properties props = new Properties();
            props.put("bootstrap.servers",bootstrapServers);
            props.put("acks",acls);
            props.put(ProducerConfig.RETRIES_CONFIG,3);

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSerializer);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, stringSerializer);

            ImmutableMap<String, Object> transactionProps = ImmutableMap.<String, Object>builder()
                    .put(ProducerConfig.BATCH_SIZE_CONFIG, 100)
                    .put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432)
                    .put(ProducerConfig.LINGER_MS_CONFIG, 1000)// Linge:逗留时间,缓存同一批次最长逗留时间;
                    .put(ProducerConfig.RETRIES_CONFIG, 3)
                    .put(ProducerConfig.CLIENT_ID_CONFIG, txProducerClientId)
                    .put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, commTransactionId)
                    .put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)// Idempotence:幂等性;
                    .build();

            props.putAll(transactionProps);

            if(overrideProps!=null && !overrideProps.isEmpty()){
                props.putAll(overrideProps);
            }

            return new KafkaProducer<>(props);
        }


        static final List<String> assets = ImmutableList.<String>builder()
                .add("asset01").add("myAsset").add("asset002")
                .add("AssetID")
                .build();

//        public static JSONObject getJsonRecord(){
//            String assetId = assets.get(random.nextInt(assets.size()));
//            return generateAnSingleRecord("orgId", "modelId", assetId, "pointId", null);
//        }

        public static String getRecordString(){
            return  getInstance().dataGenerator.getJsonRecord().toJSONString();
        }

        static final List<String> words = ImmutableList.<String>builder()
                .add("java").add("scala").add("kafka")
                .add("java").add("spark").add("hadoop")
                .add("java").add("big").add("flink")
                .add("spark")
                .build();
        static final Random random = new Random();
        public static String getWordsString(){
            StringJoiner joiner = new StringJoiner(" ");
            for (int i = 0; i < random.nextInt(8) +2; i++) {
                joiner.add(words.get(random.nextInt(words.size())));
            }
            return joiner.toString();
        }

        @Test
        public void testGetWordsString(){
            for (int i = 0; i < 100; i++) {
                System.out.println(getWordsString());
            }
        }

        protected static void sendToTopicWithNumRecords(String topic, int numRecords, String servers) {
            KafkaProducer<String, String> producer = getProducer(ImmutableMap.<String,Object>builder()
                    .put(ProducerConfig.BATCH_SIZE_CONFIG,100)
                    .put(ProducerConfig.RETRIES_CONFIG,20)
                    .put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true)
                    .build());
            try {
                for(int i=0;i<numRecords;i++){
                    producer.send(new ProducerRecord<>(topic,String.valueOf(i),String.valueOf(i)));
                }
                producer.flush();
            }finally {
                producer.close();
            }
        }

        protected static <K,V>  KafkaProducer<K, V>  createTransactionalProducer(String transactionalId, String servers) {
            KafkaProducer<K, V> transactionProducer = getTransactionProducer(ImmutableMap.of(
                    ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId,
                    ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1,
                    ProducerConfig.RETRIES_CONFIG, 1000,
                    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true
            ));

            return transactionProducer;
        }

        protected static ProducerRecord<String, String> generateStringProducerRecord(String topic, String key, String value,final boolean shouldCommit) {
            Header header = new Header() {
                @Override
                public String key() {
                    return "transactionStatus";
                }
                @Override
                public byte[] value() {
                    if (shouldCommit) {
                        return "committed".getBytes();
                    } else {
                        return "aborted".getBytes();
                    }
                }
            };
            return new ProducerRecord<String, String>(topic,null, key, value, Collections.singleton(header));
        }

        public static ExecutorService runProducerThreadInRate(String topic, int numRecords, String servers, long intervalMillis,final boolean isRecord){
            KafkaProducer<String, String> producer = getProducer(ImmutableMap.<String,Object>builder()
                    .put(ProducerConfig.BATCH_SIZE_CONFIG,100)
                    .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,servers)
                    .put(ProducerConfig.RETRIES_CONFIG,20)
                    .put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true)
                    .build());
            ScheduledExecutorService es = Executors.newScheduledThreadPool(1);
            es.scheduleAtFixedRate(()->{
                try{
                    for(int i=0;i<numRecords;i++){
                        if(isRecord){
                            producer.send(new ProducerRecord<>(topic,String.valueOf(i),getRecordString()));
                        }else {
                            producer.send(new ProducerRecord<>(topic,String.valueOf(i),getWordsString()));
                        }
                    }
                    producer.flush();
                    System.out.println(numRecords+" 个数据发送至: "+topic);
                }catch (Exception e){
                    producer.close();
                    throw e;
                }
            },0,intervalMillis, TimeUnit.MILLISECONDS);
            return es;
        }

        public static void shutdownExecutorServer(ExecutorService es) throws InterruptedException {
            es.shutdown();
            while (!es.awaitTermination(2000,TimeUnit.MILLISECONDS)){
                System.out.println("等待线程结束"+es);
                es.shutdownNow();
            }
            es = null;
            System.out.println("ExecutorService 关闭, 运行结束! ");
        }

        @Test
        public void testLoopRunProducerInFixInterval() throws InterruptedException {
            ExecutorService es = runProducerThreadInRate("testKafkaApi", 10, "ldsver51:9092", 3000,false);
            Thread.sleep(1000*10);
            shutdownExecutorServer(es);

        }



    }


    public static class StateView{
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
        private int iteration;
        private int windowIndex;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long reportingInterval;

        public StateView(long numRecords,long reportingInterval) {
            this.start= System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.iteration = 0;
            this.sampling = (int) (numRecords / Math.min(numRecords,10000*10) );
            this.latencies = new int[ (int)(numRecords / this.sampling) +1];
            this.windowIndex = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
        }

        public void recordOnce(int iter,int latency,int bytes , long time){
            this.count++;
            this.bytes+=bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency,latency);

            this.windowCount++;
            this.windowBytes +=bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(this.windowMaxLatency,latency);
            if( iter %sampling ==0){
                this.latencies[windowIndex]=latency;
                this.windowIndex++;
            }

            if(time - windowStart >= reportingInterval){
                printWindow();

                this.windowStart = System.currentTimeMillis();
                this.windowCount = 0;
                this.windowMaxLatency = 0;
                this.windowTotalLatency = 0;
                this.windowBytes = 0;
            }
        }


        public Callback nextCompletion(long start, int bytes, StateView stats){
            Callback cb = new MyCallback(this.iteration, start, bytes, stats);
            this.iteration++;
            return cb;
        }

        private void printWindow() {
            long elapse = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) elapse;

            System.out.printf("窗口:%d,\t 数量:%d,\t QPS:%.2f (%.4f万),\t\t 单Record均用时:%.2f毫秒 ,\t 窗口内MaxLatency:%.3f  \n",
                    windowIndex,
                    windowCount,
                    recsPerSec,
                    recsPerSec/10000,
                    windowTotalLatency / (double) windowCount,
                    (double) windowMaxLatency);
        }


        public void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;

            System.out.printf("总record数:%d,\t QPS:%.2f (%.4f万) 个/sec,\t 平均速率:%.2f mills/个,   MaxLatency:%.2f \n",
                    count,
                    recsPerSec,
                    recsPerSec/10000,
                    totalLatency / (double) count,
                    (double) maxLatency
            );
        }

    }

    public static class MyCallback implements Callback{
        private final long start;
        private final int iteration;
        private final int bytes;
        private final StateView stats;


        public MyCallback(int iter, long start, int bytes, StateView stats) {
            this.start = start;
            this.stats = stats;
            this.iteration = iter;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            this.stats.recordOnce(iteration, latency, bytes, now);
            if (exception != null)
                exception.printStackTrace();
        }

    }


    @Test
    public void testArgsParse() throws ArgumentParserException {
        String[] args = {"--master","local[2]",
//                "--topics","testKafkaApi",
//                "--brokerServers","ldsver51:9092",
                "--consumerGroup","spark-streaming-test"
        };




        Namespace res = getNamespaceByArgsParser(args, ImmutableMap.of(
                "brokerServers", "ldsver51:9092",
                "topics", "testKafkaApi",
                "consumerGroup", "group-spark-streaming",
                "master", "local[3]"
        ));

        String master = res.getString("master");
        String brokerServers = res.getString("brokerServers");
        String topics = res.getString("topics");
        String consumerGroup = res.getString("consumerGroup");

        System.out.println(res);


    }



}
