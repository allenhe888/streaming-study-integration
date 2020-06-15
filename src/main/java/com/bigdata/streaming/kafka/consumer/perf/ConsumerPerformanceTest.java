package com.bigdata.streaming.kafka.consumer.perf;

import com.bigdata.streaming.common.CommKey;
import com.bigdata.streaming.common.StaticCounter;
import com.bigdata.streaming.kafka.common.KafkaDevHelper;
import com.bigdata.streaming.kafka.consumer.api.ConsumerApiDemo;
import com.bigdata.streaming.kafka.consumer.consumerperf.ConsumerPerfDemo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;

public class ConsumerPerformanceTest extends KafkaDevHelper{

    int recordNum = 10000 * 1000;
    String topic = "HJQ_testKafkaPerPartition";// HJQ_testKafkaPerPartition, testStringPerf,

    @Test
    public void testStringPerfBySubscribeType(){
        KafkaConsumer<String,String> consumer= getKafkaConsumer(ImmutableMap.of(
                "key.deserializer",stringDeserializer,
                "value.deserializer",stringDeserializer
        ));
//        performanceHelper.doSubscribeTypePerformanceTestAndClose(consumer,recordNum,3000,topic);
        performanceHelper.loopPerformanceTestInSubscribeType(consumer,topic,0,recordNum,5000,10,4);
    }

    @Test
    public void testStringPerfByAssignType(){
         /*
        --bootstrapServer ldsver51:9092 --topic testStringPerf --recordNum 6000000
         */
        String argsStr = "--bootstrapServer ldsver51:9092 --topic testStringPerf --recordNum 6000000";
        new ConsumerPerformanceTest().doAssignPerformanceTest(argsStr.split(CommKey.EMPTY_STRING));
    }

    @Test
    public void testStringPerfAssign_maxPollRec500_fetchBytes1M(){
        String argsStr = "--bootstrapServer ldsver51:9092 --topic testStringPerf --maxPollRecords 500 --maxPartitionFetchBytes 1048576 --recordNum 4000000";
        new ConsumerPerformanceTest().doAssignPerformanceTest(argsStr.split(CommKey.EMPTY_STRING));
    }

    public void doLoopPerformanceTestByAssignType(String bootstrapServers,String topic,String partitions,String groupId,int maxPollRecords,
                                                  int maxBytes,int startOffset,int recordNum,int batchIntervalSec,int reportIntervalSec){
        String[] split = partitions.split(",");
        HashSet<Integer> partitionNumSet = new HashSet<>();
        ArrayList<TopicPartition> tps = new ArrayList<>();
        for(String num:partitions.split(",")){
            int part = Integer.parseInt(num);
            if(!partitionNumSet.contains(part)){
                partitionNumSet.add(part);
                tps.add(new TopicPartition(topic, part));
            }
        }

        KafkaConsumer<String,String> consumer= getKafkaConsumer(ImmutableMap.<String,Object>builder()
                .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                .put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                .put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords)
                .put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(maxBytes))
                .put("key.deserializer", stringDeserializer)
                .put("value.deserializer", stringDeserializer)
                .build());
        StaticCounter.getInstance().createThreadTimeAndBatchCount();

        performanceHelper.loopPerformanceTestInAssignType(consumer, tps,startOffset,recordNum,5000,batchIntervalSec,reportIntervalSec);
        StaticCounter.getInstance().removeListThreadLocal();
    }

    public void doAssignPerformanceTest(String[] args){
        Namespace namespace = getNamespaceByArgsParser(args, ImmutableMap.<String, String>builder()
                .put(CommKey.bootstrapServers, "ldsver51:9092")
                .put(CommKey.topic, "testStringPerf")
                .put(CommKey.partitions, "0")
                .put(CommKey.groupId, "java-consumer-assign-perf")
                .put(CommKey.maxPollRecords, String.valueOf(500))
                .put(CommKey.maxPartitionFetchBytes, String.valueOf(1048576))
                .put(CommKey.startOffset, "0")
                .put(CommKey.recordNum, String.valueOf(10000 * 800))
                .put(CommKey.batchIntervalSec, "10")
                .put(CommKey.reportIntervalSec, "4")
                .build());

        doLoopPerformanceTestByAssignType(namespace.getString(CommKey.bootstrapServers),
                namespace.getString(CommKey.topic),
                namespace.getString(CommKey.partitions),
                namespace.getString(CommKey.groupId),
                Integer.parseInt(namespace.getString(CommKey.maxPollRecords)),
                Integer.parseInt(namespace.getString(CommKey.maxPartitionFetchBytes)),
                Integer.parseInt(namespace.getString(CommKey.startOffset)),
                Integer.parseInt(namespace.getString(CommKey.recordNum)),
                Integer.parseInt(namespace.getString(CommKey.batchIntervalSec)),
                Integer.parseInt(namespace.getString(CommKey.reportIntervalSec)));

    }


    @Test
    public void testBytesPerf(){
        String topic = "testStringPerf";// HJQ_testKafkaPerPartition, testStringPerf,
        int recordNum = 10000 * 800;
        KafkaConsumer<byte[],byte[]> consumer= getKafkaConsumer(ImmutableMap.of(
                "key.deserializer", BytesDeserializer.class.getName(),
                "value.deserializer",BytesDeserializer.class.getName()
        ));
//        performanceHelper.doSubscribeTypePerformanceTestAndClose(consumer,recordNum, 3000,topic);
        performanceHelper.loopPerformanceTestInAssignType(consumer, ImmutableList.of(new TopicPartition(topic,1)),0,recordNum,5000,10,2);

    }


    public static void main(String[] args) {
        new ConsumerPerformanceTest().doAssignPerformanceTest(args);
    }

    @Test
    public void testMainInDoAssignPerformanceTest(){
        /*
        java -cp /home/eos/testPerf/jars/streaming-study-integration-1.0-SNAPSHOT.jar  com.bigdata.streaming.kafka.consumer.perf.ConsumerPerformanceTest
         --bootstrapServer kafka9001.eniot.io:9092,kafka9002.eniot.io:9092,kafka9003.eniot.io:9092
        --topic HJQ_testKafkaPerPartition_01 --startOffset 12500453
         */
        String argsStr = "--bootstrapServer kafka9001.eniot.io:9092,kafka9002.eniot.io:9092,kafka9003.eniot.io:9092";
        new ConsumerPerformanceTest().doAssignPerformanceTest(argsStr.split(CommKey.EMPTY_STRING));
    }

    @Test
    public void testConsumerPerformance(){
        String argsStr = "ldsver51:9092 HJQ_testKafkaPerPartition ideaTestGroup 5000000";
        new ConsumerPerformanceTest().doKafkaConsumerPerformance(argsStr.split(CommKey.EMPTY_STRING));
    }

    private void doKafkaConsumerPerformance(String[] args) {
        String brokerList= "ldsver51:9092";
        String stringTopic= "HJQ_testKafkaPerPartition";
        String groupPrefix= "ideaTestGroup";
        int numRecords= 1000000;
        if(args.length>0){
            brokerList = args[0];
        }
        if(args.length>1){
            stringTopic = args[1];
        }
        if(args.length>2){
            groupPrefix = args[2];
        }
        if(args.length>3){
            numRecords = Integer.parseInt(args[3]);
        }
//        String[] mainArgs = KafkaDevHelper.parsedConsumerArags(args);
//        String brokerList= mainArgs[0];
//        String stringTopic= mainArgs[1];
//        String groupPrefix= mainArgs[2];
//        int numRecords= Integer.parseInt(mainArgs[3]);
        int recordByteSize= KafkaDevHelper.getRecordString().getBytes().length;
        System.err.println("\n\nC.1 consumer-perf脚本: ByteArrayDesc \t-\t topic="+stringTopic+" \t-\t recordSize="+numRecords+" \t-\t String.size="+recordByteSize);
        ConsumerPerfDemo.doMain(brokerList,stringTopic,groupPrefix+"_bytes",numRecords);
        KafkaDevHelper.ConsumerHelp.previewRunningForResetBeginningOffsets(brokerList,stringTopic,groupPrefix);
        System.err.println("\n\nC.2 ConsumerApiDemo: StringDesc \t-\t topic="+stringTopic+" \t-\t recordSize="+numRecords+" \t-\t String.size="+recordByteSize);
        new ConsumerApiDemo.TestPerformance().testConsumerPerformacne_DeseByString(brokerList,stringTopic,groupPrefix+"_string",numRecords);
        sleepForClose();
        System.err.println("\n\nC.3 ConsumerApiDemo: JsonDesc \t-\t topic="+stringTopic+" \t-\t recordSize="+numRecords+" \t-\t Json.size="+recordByteSize);
        new ConsumerApiDemo.TestPerformance().testConsumerPerformacne_DirectToJson(brokerList,stringTopic,groupPrefix+"_json",numRecords);
        sleepForClose();

    }

    @Test
    public void testByteArrayPerformance() {
        String argsStr = "ldsver51:9092 HJQ_testKafkaPerPartition ideaTestGroup 5000000";
        String[] args =argsStr.split(CommKey.EMPTY_STRING);
        String brokerList= "ldsver51:9092";
        String stringTopic= "HJQ_testKafkaPerPartition";
        String groupPrefix= "ideaTestGroup";
        int numRecords= 5000000;

        if(args.length>0){
            brokerList = args[0];
        }
        if(args.length>1){
            stringTopic = args[1];
        }
        if(args.length>2){
            groupPrefix = args[2];
        }
        if(args.length>3){
            numRecords = Integer.parseInt(args[3]);
        }

        int recordByteSize= KafkaDevHelper.getRecordString().getBytes().length;
        System.err.println("\n\nC.1 consumer-perf脚本: ByteArrayDesc \t-\t topic="+stringTopic+" \t-\t recordSize="+numRecords+" \t-\t String.size="+recordByteSize);
        ConsumerPerfDemo.doMain(brokerList,stringTopic,groupPrefix+"_bytes",numRecords);

    }

    @Test
    public void testStringMsgConsumerPerformance() {
        String argsStr = "ldsver51:9092 HJQ_testKafkaPerPartition ideaTestGroup 5000000";
        String[] args =argsStr.split(CommKey.EMPTY_STRING);
        String brokerList= "ldsver51:9092";
        String stringTopic= "HJQ_testKafkaPerPartition";
        String groupPrefix= "ideaTestGroup";
        int numRecords= 5000000;
        if(args.length>0){
            brokerList = args[0];
        }
        if(args.length>1){
            stringTopic = args[1];
        }
        if(args.length>2){
            groupPrefix = args[2];
        }
        if(args.length>3){
            numRecords = Integer.parseInt(args[3]);
        }

        int recordByteSize= KafkaDevHelper.getRecordString().getBytes().length;

        System.err.println("\n\nC.2 ConsumerApiDemo: StringDesc \t-\t topic="+stringTopic+" \t-\t recordSize="+numRecords+" \t-\t String.size="+recordByteSize);
        new ConsumerApiDemo.TestPerformance().testConsumerPerformacne_DeseByString(brokerList,stringTopic,groupPrefix+"_string",numRecords);

    }



    private static void sleepForClose() {
        try {
            Thread.sleep(3000L);
        } catch (InterruptedException e) {e.printStackTrace();}
    }

}
