package com.bigdata.streaming.kafka.consumer.api;

import com.alibaba.fastjson.JSONObject;
import com.bigdata.streaming.kafka.common.KafkaDevHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

;

public class ConsumerApiDemo extends KafkaDevHelper {

    @Test
    public void testHighLevelConsumer(){
        KafkaConsumer<byte[],byte[]> consumer= getKafkaConsumer(ImmutableMap.of());
        consumer.subscribe(Collections.singleton(topic));
        consumer.partitionsFor(topic);
        ConsumerRecords<byte[],byte[]> poll = consumer.poll(100);
        for(ConsumerRecord<byte[],byte[]> record:poll){
            System.out.println("key="+new String(record.key()) +" , value="+new String(record.value()));
        }
    }

    public static class TestSerializer{

        @Test
        public void testByteArraySerializer(){
            String serializer = "org.apache.kafka.common.serialization.StringSerializer";
            String topic = "testCreate01";
            KafkaConsumer<String,String> consumer= getKafkaConsumer(ImmutableMap.of(
                    "group.id","testGroup-01",
                    "key.deserializer",serializer,
                    "value.deserializer",serializer)
            );
            consumer.subscribe(Collections.singleton(topic));
//            consumer.partitionsFor(topic);

            for(int i=0;i<10;i++){
                ConsumerRecords<String,String> poll = consumer.poll(1000);
                System.out.println(i+"次poll: ");
                for(ConsumerRecord<String,String> record:poll){
                    String key = record.key();
                    String value = record.value();
                    System.out.println(i+"次poll:\t key="+ key.toString() +" ,value="+value.toString());
                }

            }


        }

    }

    public static class TestPerformance{
        int recordNum = 10000 * 100;

        @Test
        public void testConsumerPerformacne_DeseByArrayByte(){
            String topic = "testBytePerfRecord";
            KafkaConsumer<byte[],byte[]> consumer= getKafkaConsumer(ImmutableMap.of(
                    "key.deserializer",byteDeserializer,
                    "value.deserializer",byteDeserializer
            ));

            testConsumerRate(consumer,recordNum, 100,topic);
        }

        @Test
        public void testConsumerPerformacne_DeseByString(){
            String topic = "testStringPerf";
            KafkaConsumer<String,String> consumer= getKafkaConsumer(ImmutableMap.of(
                    "key.deserializer",stringDeserializer,
                    "value.deserializer",stringDeserializer
            ));
            testConsumerRate(consumer,recordNum, 3000,topic);
        }

        @Test
        public void testStringPerf(){
            String topic = "testStringPerf";
            KafkaConsumer<String,String> consumer= getKafkaConsumer(ImmutableMap.of(
                    "key.deserializer",stringDeserializer,
                    "value.deserializer",stringDeserializer
            ));
            testConsumerRate(consumer,recordNum, 3000,topic);
        }
        @Test
        public void testBytesPerf(){
            String topic = "testStringPerf";
            KafkaConsumer<byte[],byte[]> consumer= getKafkaConsumer(ImmutableMap.of(
                    "key.deserializer", BytesDeserializer.class.getName(),
                    "value.deserializer",BytesDeserializer.class.getName()
            ));
            testConsumerRate(consumer,recordNum, 3000,topic);
        }

        public void testConsumerPerformacne_DeseByString(String brokerList,String topic,String consumerGroup,int numRecords){
            KafkaConsumer<String,String> consumer= getKafkaConsumer(ImmutableMap.of(
                    "bootstrap.servers",brokerList,
                    "group.id", consumerGroup,
                    "key.deserializer",stringDeserializer,
                    "value.deserializer",stringDeserializer
            ));
            testConsumerRate(consumer,numRecords, 5000,topic);

        }

        @Test
        public void testConsumerPerformacne_DirectToJson(){
            String jsonDeserializer = JsonDeserializer.class.getName();
            KafkaConsumer<String, JSONObject> consumer= getKafkaConsumer(ImmutableMap.of(
                    "key.deserializer",stringDeserializer,
                    "value.deserializer",jsonDeserializer
            ));
            consumer.subscribe(Collections.singleton(topic));
            consumer.partitionsFor(topic);
            testConsumerRate(consumer,recordNum, 4000,topic);

        }

        public void testConsumerPerformacne_DirectToJson(String brokerList,String topic,String consumerGroup,int numRecords){
            String jsonDeserializer = JsonDeserializer.class.getName();
            KafkaConsumer<String, JSONObject> consumer= getKafkaConsumer(ImmutableMap.of(
                    "bootstrap.servers",brokerList,
                    "group.id", consumerGroup,
                    "key.deserializer",stringDeserializer,
                    "value.deserializer",jsonDeserializer
            ));
            testConsumerRate(consumer,numRecords, 4000,topic);


        }



    }


    public static class TestKafkaConsumerApi{
        KafkaConsumer<String,String> consumer;
        Set<TopicPartition> partitions;
        String topic = "HJQ_testKafkaPerPartition";

        private <K,V> KafkaConsumer<K,V> getDefaultStringConsumer(boolean newGroup){
            HashMap<String, Object> kafkaProps = new HashMap<>();
            kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
            kafkaProps.put("key.deserializer",stringDeserializer);
            kafkaProps.put("value.deserializer",stringDeserializer);
            if(newGroup){
                kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG,groupId+"_"+(new Random().nextInt()));
            }
            kafkaProps.put("value.deserializer",stringDeserializer);
            KafkaConsumer<K,V> consumer= getKafkaConsumer(kafkaProps);
            return consumer;
        }

        @Test
        public void runTestSucceed(){
            KafkaConsumer<String,String> consumer= getDefaultStringConsumer(false);
            consumer.subscribe(Collections.singleton("testTopic"));
            ConsumerHelp.doConsumerInFixRate(consumer, true,1000,500);
            consumer.close();
        }


        @Test
        public void testPosition(){
            KafkaConsumer<String,String> consumer= getDefaultStringConsumer(false);
            consumer.subscribe(Collections.singleton(topic));
            ConsumerHelp.doConsumer(consumer, 1000, 3000L, true);

            consumer.poll(0);
            Set<TopicPartition> assignment = consumer.assignment();
            assignment.forEach(tp->{
                long position = consumer.position(tp);
                System.out.println(tp+" -> "+position);
            });

            System.err.println("seekToEnd 后");
            consumer.seekToEnd(assignment);
            assignment.forEach(tp->{
                long position = consumer.position(tp);
                System.out.println(tp+" -> "+position);
            });

            consumer.close();
        }

        @Test
        public void testLoopSeekToEndAndPosition(){
            KafkaConsumer<String,String> consumer= getDefaultStringConsumer(false);
            consumer.subscribe(Collections.singleton(topic));
            HashMap<TopicPartition, Long> map = new HashMap<>();

            try{
                while (true){
                    consumer.poll(500);
                    Set<TopicPartition> assignment = consumer.assignment();
                    consumer.seekToEnd(assignment);

                    assignment.forEach(tp->{
                        long position = consumer.position(tp);
                        Long lastOffset = map.get(tp);
                        if(null!=lastOffset && position!=lastOffset){
                            System.out.println(tp+"分区 发生了offset变化:  "+lastOffset+" -> "+position+" ="+(position - lastOffset));
                        }else if(null==lastOffset) {
                            System.out.println(tp+"分区 第一次读取时 offset:  "+position);
                        }
                        map.put(tp,position);
                    });

                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {}
                }
            }finally {
                consumer.close();
            }

        }

        @Test
        public void testConsumerGroupRebalance(){
            doConsumerGroupRebalanceTest("HJQ_testKafkaPerPartition","sparkStreamingGroup");
        }


        public void doConsumerGroupRebalanceTest(String topic,String groupId){
            KafkaConsumer<String, String> kafkaConsumer = KafkaDevHelper.getKafkaConsumer(ImmutableMap.of(
                    ConsumerConfig.GROUP_ID_CONFIG, groupId,
                    ConsumerConfig.CLIENT_ID_CONFIG, "client_2-kafkaApi-demo"
            ));
            // 用这个自动订阅, 会把
            kafkaConsumer.subscribe(Collections.singleton(topic));
            ConsumerHelp.doConsumerInFixRate(kafkaConsumer, true,20000,2000);
            consumer.close();
        }

        @Test
        public void testConsumerGroupRebalanceByPollTimeout(){
            String groupId = "sparkStreamingGroup";
            String topic = "HJQ_testKafkaPerPartition";
            int maxPollInterval = 10000;
            KafkaConsumer<String, String> kafkaConsumer = KafkaDevHelper.getKafkaConsumer(ImmutableMap.of(
                    ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000",
                    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollInterval),//10秒
//                    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "1000",

                    ConsumerConfig.GROUP_ID_CONFIG, groupId
            ));
            // 用这个自动订阅, 会把
            kafkaConsumer.subscribe(Collections.singleton(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    System.out.println("准备Rebalance");
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.println("完成Rebalance");
                }
            });
            ConsumerHelp.doAssignTP(kafkaConsumer,100);

            try {
                for (int i = 0; i < 10000; i++) {
                    if(i%5==0){

                        System.out.println("当前批次"+i+", 进入"+maxPollInterval*2/1000+" 秒钟的休眠");
                        Thread.sleep(maxPollInterval*2);
                        System.out.println("休眠结束");

                        ConsumerRecords<String, String> poll = kafkaConsumer.poll(500);
                        System.out.println(i+" 批次: 等待"+ maxPollInterval*2 +" 后 poll到数据量: "+poll.count());
                    }else {
                        kafkaConsumer.poll(500);
                    }
                    Thread.sleep(1000);
                }
            } catch (Exception e) {e.printStackTrace();}

            consumer.close();
        }


    }


    public static class TestConsumerApi{
        KafkaConsumer<String,String> consumer;
        Set<TopicPartition> partitions;
        long startTime;

        @Before
        public void beforeTest(){
            startTime = System.currentTimeMillis();
        }

        private <K,V> KafkaConsumer<K,V> getDefaultStringConsumer(){
            KafkaConsumer<K,V> consumer= getKafkaConsumer(ImmutableMap.of(
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false,
                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG,50,
                    "key.deserializer",stringDeserializer,
                    "value.deserializer",stringDeserializer
            ));
            return consumer;
        }

        private <K,V> Set<TopicPartition> doAssignTP(KafkaConsumer<K,V> consumer){
            Set<TopicPartition> assignment = ConsumerHelp.doAssignTP(consumer,100);
            return assignment;
        }

        private <K,V> KafkaConsumer<K,V> getConsumerAndDoAssign(Map<String,Object> overrideProps,String topic){
            KafkaConsumer<K,V> consumer= getKafkaConsumer(overrideProps);
            consumer.subscribe(Collections.singleton(topic));
            partitions = doAssignTP(consumer);
            return consumer;
        }

        @Test
        public void testCommit_enableAutoCommit(){
            consumer= getConsumerAndDoAssign(ImmutableMap.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"50",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true,
                    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000"
            ),topic);

            int numRecords = 50*4;
            long resetOffset = 2188;
            List<ConsumerRecord<String, String>> records = ConsumerHelp.doConsumer(consumer, numRecords, 3000L, true);
            consumer.close();

            consumer = getConsumerAndDoAssign(ImmutableMap.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"50",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true,
                    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000"
            ),topic);
            List<ConsumerRecord<String, String>> records2 = ConsumerHelp.doConsumer(consumer, numRecords, 3000L, true);
            for(int i=0;i<records.size();i++){
                ConsumerRecord<String, String> data1 = records.get(i);
                ConsumerRecord<String, String> data2 = records2.get(i);
                if(data1.partition()==data2.partition()){
                    Assert.assertEquals(data1.offset()+numRecords,data2.offset());
                }

            }

        }

        @Test
        public void testCommitPerf_enableAutoCommit(){
            consumer= getConsumerAndDoAssign(ImmutableMap.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"500",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true,
                    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000"
            ),topic);

            int numRecords = 10000*3000;
            consumer.seekToBeginning(partitions);
            ConsumerHelp.doConsumerPerformance(consumer, numRecords, 3000L, true,2000);
        }


        @Test
        public void testCommit_NotAutoCommit_SyncInFixSize(){
            consumer= getConsumerAndDoAssign(ImmutableMap.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"50",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false,
                    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000"
            ),topic);

            int numRecords = 50*4;
            long timeout = 2000L;
            int minCommitSize=200;
            int reportingInterval = 2000;

            long start =  System.currentTimeMillis();
            long lastDataTime = System.currentTimeMillis();
            ConsumerRecords<String,String> poll=null;
            AtomicLong pollCount = new AtomicLong(0);
            long currentTime = System.currentTimeMillis();

            long lastReportTime = currentTime;
            int lastReportRecordSize = 0;
            int totalSize = 0;

            int lastCommitRecordSize= 0;
            List<ConsumerRecord<String, String>> buff = new ArrayList<>();
            while (totalSize< numRecords && System.currentTimeMillis() - lastDataTime <= timeout){
                currentTime=System.currentTimeMillis();
                poll = consumer.poll(100);
                if(poll!=null && !poll.isEmpty()){
                    lastDataTime = currentTime;
                    totalSize += poll.count();
                    poll.forEach(r->buff.add(r));

                    if(buff.size() - lastCommitRecordSize >= minCommitSize){
                        insertIntoDB(buff);
                        consumer.commitSync();
                        buff.clear();
                    }

                    if(currentTime - lastReportTime >= reportingInterval){
                        printQPSPerBatch(totalSize - lastReportRecordSize,currentTime - lastReportTime);
                        ConsumerHelp.printPollConsumerRecords(poll,pollCount);
                        lastReportTime = currentTime;
                        lastReportRecordSize = totalSize;
                    }
                }

            }
            printQPSPerBatch(totalSize ,currentTime - start);
        }

        private void insertIntoDB(List<ConsumerRecord<String, String>> buff) {
            buff.forEach(data->{
                data=null;
            });
        }


        @Test
        public void testNoCommit(){
            KafkaConsumer<String,String> consumer= getKafkaConsumer(ImmutableMap.of(
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false,
                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG,50,
                    "key.deserializer",stringDeserializer,
                    "value.deserializer",stringDeserializer
            ));

            consumer.subscribe(Collections.singleton(topic));
            Set<TopicPartition> assignment = doAssignTP(consumer);
            int numRecords = 50*1000;
            ConsumerHelp.doConsumer(consumer, numRecords, 2000L, true);
            consumer.close();
        }

        @Test
        public void testFromBeginningFromSpecifyOffset_seek(){
            KafkaConsumer<String,String> consumer= getKafkaConsumer(ImmutableMap.of(
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false,
                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG,50,
                    "key.deserializer",stringDeserializer,
                    "value.deserializer",stringDeserializer
            ));
            consumer.subscribe(Collections.singleton(topic));
            Set<TopicPartition> assignment = doAssignTP(consumer);

            int beginOffset=500;
            assignment.forEach(tp->{
                consumer.seek(tp,beginOffset);
                System.out.println(tp+" 从 "+beginOffset+"开发系哦啊方");
            });

            List<ConsumerRecord<String, String>> consumerRecords = ConsumerHelp.doConsumer(consumer, 10000, 3000L, true);
            consumer.close();
        }

        @Test
        public void testFromZoneOffset_beginningOffsets(){
            int numRecords = 50*4;
            String topic = "HJQ_testKafkaPerPartition";
            KafkaConsumer<String,String> consumer= getDefaultStringConsumer();
            consumer.subscribe(Collections.singleton(topic));
            Set<TopicPartition> assignment = doAssignTP(consumer);

            Map<TopicPartition, Long> beginOffsets = consumer.beginningOffsets(assignment);
            System.out.println("beginOffsets: "+beginOffsets);
            List<ConsumerRecord<String, String>> consumerRecords = ConsumerHelp.doConsumer(consumer, numRecords, 3000L, true);
            System.err.println("seek(beginOffsets) ");
            beginOffsets.forEach((k,v)->{
                consumer.seek(k,v);
                System.out.println("k="+k+", v="+v);
            });
            List<ConsumerRecord<String, String>> records = ConsumerHelp.doConsumer(consumer, numRecords, 3000L, true);
            Assert.assertEquals(records.size(),numRecords);
            Assert.assertEquals(records.get(0).offset(),0);
            Assert.assertEquals(records.get(numRecords -1).offset(),numRecords -1);
            consumer.close();
        }


        @Test
        public void testFromZoneOffset_DevHelperTest(){
            ConsumerHelp.previewRunningForResetBeginningOffsets(brokerList,"testStringPerf","testPerfGroup");
        }


        @Test
        public void  TestPreviewRunningForResetBeginningOffsets(){

            KafkaDevHelper.ConsumerHelp.previewRunningForResetBeginningOffsets(brokerList,"testStringPerf","testGroup");

        }

        @Test
        public void testFromEndToPoll_endOffsets(){// consumer.seekToEnd()从最末尾的offset消费其, 基本没啥用处吧?
            int numRecords = 50*4;
            KafkaConsumer<String,String> consumer= getDefaultStringConsumer();
            consumer.subscribe(Collections.singleton(topic));
            Set<TopicPartition> assignment = doAssignTP(consumer);

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
            System.out.println("endOffsets: "+endOffsets);
            endOffsets.forEach((k,v)->{
                consumer.seek(k,v);
            });
            List<ConsumerRecord<String, String>> consumerRecords = ConsumerHelp.doConsumer(consumer, numRecords, 3000L, true);

            consumer.seekToEnd(assignment);
            ConsumerHelp.doConsumer(consumer, numRecords, 3000L, true);


            consumer.seekToBeginning(assignment);
            ConsumerHelp.doConsumer(consumer, numRecords, 3000L, true);

        }

        @Test
        public void testSeekSpecifyOffset_afterResetOfferOnHand(){
            int numRecords = 50*4;
            long resetOffset = 2188;
            KafkaConsumer<String,String> consumer= getDefaultStringConsumer();
            consumer.subscribe(Collections.singleton(topic));
            Set<TopicPartition> assignment = doAssignTP(consumer);

            List<ConsumerRecord<String, String>> records = ConsumerHelp.doConsumer(consumer, numRecords, 3000L, true);
            Assert.assertEquals(records.size(),numRecords);
            Assert.assertEquals(records.get(0).offset(),resetOffset);
            Assert.assertEquals(records.get(numRecords -1).offset(),numRecords +resetOffset-1);

        }

        @Test
        public void testSeekSpecifyTimestamp_offsetsForTimes(){
            int numRecords = 50*4;
            long resetOffset = 2188;
            KafkaConsumer<String,String> consumer= getDefaultStringConsumer();
            consumer.subscribe(Collections.singleton(topic));
            Set<TopicPartition> assignment = doAssignTP(consumer);

            Map<TopicPartition, Long> partitionLongMap = new HashMap<>();
            long startTimestamp = System.currentTimeMillis() - 1000*3600*48;
            assignment.forEach(tp->partitionLongMap.put(tp,startTimestamp));

            // offsetsForTimestamp, 返回时间戳大于等于查询时间的第一条消息对应的 offset 和 timestamp
            Map<TopicPartition, OffsetAndTimestamp> offsetAndTime = consumer.offsetsForTimes(partitionLongMap);
            System.out.println("offsetAndTime:\t"+offsetAndTime);
            offsetAndTime.forEach((k,v)-> System.out.println(k+" -> "+v));
            ConsumerHelp.doConsumer(consumer, numRecords, 3000L, true);

            System.out.println(" seek(offsetsForTimes())完后:\n");
            offsetAndTime.forEach((k,v)->{
                consumer.seek(k,v.offset());
            });

            List<ConsumerRecord<String, String>> records = ConsumerHelp.doConsumer(consumer, numRecords, 3000L, true);
            ConsumerRecord<String, String> first = records.get(0);
            long queryFirstRecordTime = first.timestamp();
            long searchPartitonTime = offsetAndTime.get(new TopicPartition(topic, first.partition())).timestamp();
            Assert.assertEquals(queryFirstRecordTime,searchPartitonTime);

            long queryFirstRecordOffset = first.offset();
            long searchPartitionOffset = offsetAndTime.get(new TopicPartition(topic, first.partition())).offset();
            Assert.assertEquals(queryFirstRecordOffset,searchPartitionOffset);



        }



        @After
        public void afterTest(){
            ConsumerHelp.closeConsumer(consumer);
            System.err.println("After执行完, 总用时: "+((System.currentTimeMillis() - startTime)/1000));
        }

    }

    public static class TestAssignApi{
        String topic = "HJQ_testKafkaPerPartition";

        /*
        使用“自动”subscribe和“手动”assign的方式
           关于subscribe: 自动
            - 消费者组有个重要的功能：rebalancing。当一个新的消费者加入一个组，如果还有有效的分区（消费者数<=主题分区数），会开始一个重新均衡分配的操作
            - 保证: 所有的分区都能被(存活的)consumer消费到;

           关于assing:收到
            - assign方法由用户直接手动consumer实例消费哪些具体分区
            - assign的consumer不会拥有kafka的group management机制，也就是当group内消费者数量变化的时候不会有reblance行为发生。
         */

        @Test
        public void testSubscribe(){
            KafkaConsumer<String, String> kafkaConsumer = KafkaDevHelper.getKafkaConsumer(ImmutableMap.of(
                    ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000",
//                    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollInterval),//10秒
//                    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "1000",
                    ConsumerConfig.GROUP_ID_CONFIG, groupId
            ));
            // 用这个自动订阅, 会把
            kafkaConsumer.subscribe(Collections.singleton(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    System.out.println("准备Rebalance");
                }
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.println("完成Rebalance");
                }
            });
            Set<TopicPartition> tps = ConsumerHelp.doAssignTP(kafkaConsumer, 100);
            tps.forEach(tp->{
                tp.toString();
            });

            ArrayList<Integer> emptyBatch = new ArrayList<>();
            Random random = new Random();
            for (int i = 0; i < 20000; i++) {
                ConsumerRecords<String, String> poll = kafkaConsumer.poll(500);
                if(poll.count()>0){
                    try {
                        Thread.sleep(2+random.nextInt(8));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }else{
                    System.out.println(i+" batch时,poll=空");
                    emptyBatch.add(i);
                }
                if(i%20==0){
                    System.err.println("已发现这些batch poll()时为空; "+emptyBatch);
                }
            }

        }

        private KafkaConsumer<String, String>  newConsumer(){
            KafkaConsumer<String, String> consumer = KafkaDevHelper.getKafkaConsumer(ImmutableMap.of(
//                        ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000",
//                    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollInterval),//10秒
//                    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "1000",
                    ConsumerConfig.GROUP_ID_CONFIG, groupId
            ));
            return consumer;
        }

        @Test
        public void testAssignSucceed(){
            try{
                KafkaConsumer<String, String> consumer = KafkaDevHelper.getKafkaConsumer(ImmutableMap.of(
//                        ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000",
//                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100",
//                    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollInterval),//10秒
//                    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "1000",
                     ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,String.valueOf(1024*1024*10) ,//10M,1M=4034个,10M=4万个Record
                        ConsumerConfig.GROUP_ID_CONFIG, groupId
                ));
//                TopicPartition tp = new TopicPartition(topic, 1);
                List<TopicPartition> tps = ImmutableList.of(
//                        new TopicPartition(topic, 1),
//                        new TopicPartition(topic, 2),
                        new TopicPartition(topic, 3)
                );
                consumer.assign(tps);
                consumer.seekToBeginning(tps);

                ConsumerHelp.doConsumerInFixRate(consumer, true,10000,1000);
                consumer.close();

            }catch (Exception e){
                e.printStackTrace();
            }

        }

        @Test
        public void testAssign(){
            try{
                KafkaConsumer<String, String> consumer = KafkaDevHelper.getKafkaConsumer(ImmutableMap.of(
//                        ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000",
//                    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollInterval),//10秒
//                    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "1000",
                        ConsumerConfig.GROUP_ID_CONFIG, groupId
                ));
                TopicPartition tp = new TopicPartition(topic, 2);
                consumer.assign(Collections.singleton(tp));
                consumer.seek(tp,200);

                ArrayList<Integer> emptyBatch = new ArrayList<>();
                Random random = new Random();

                for (int i = 0; i < 20000; i++) {
                    ConsumerRecords<String, String> poll = consumer.poll(500);

                    if(poll.count()>0){
                        Thread.sleep(1+random.nextInt(5));
                    }else{
                        System.out.println(i+" batch时,poll=空");
                        emptyBatch.add(i);
                    }
                    if(i%20==0){
                        System.err.println("已发现这些batch poll()时为空; "+emptyBatch);
                    }
                }

            }catch (Exception e){
                e.printStackTrace();
            }

        }

//        @Test(expected = IllegalStateException.class)
        @Test
        public void testPollWithEmptyUserAssignment() {
            KafkaConsumer<String, String> consumer = newConsumer();
            consumer.assign(Collections.<TopicPartition>emptySet());
            try {
                consumer.poll(0);
            } finally {
                consumer.close();
            }
        }

        @Test
        public void testAssignFailAndRetry(){
            try{
                KafkaConsumer<String, String> consumer = KafkaDevHelper.getKafkaConsumer(ImmutableMap.of(
//                        ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000",
//                    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollInterval),//10秒
//                    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "1000",
                        ConsumerConfig.GROUP_ID_CONFIG, groupId
                ));
                TopicPartition tp = new TopicPartition(topic, 2);
                consumer.assign(Collections.singleton(tp));
                consumer.seek(tp,200);

                ConsumerHelp.doConsumerInFixRate(consumer, true,1000,500);
                consumer.close();

            }catch (Exception e){
                e.printStackTrace();
            }

        }

        @Test
        public void testAssignPollTimeout(){
            String groupId = "sparkStreamingGroup";
            String topic = "HJQ_testKafkaPerPartition";
            int maxPollInterval = 5000;
            KafkaConsumer<String, String> kafkaConsumer = KafkaDevHelper.getKafkaConsumer(ImmutableMap.of(
                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10",
                    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollInterval),//10秒
//                    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "1000",
                    ConsumerConfig.GROUP_ID_CONFIG, groupId
            ));
            // 用这个自动订阅, 会把
            TopicPartition tp = new TopicPartition(topic, 1);
            kafkaConsumer.assign(Collections.singleton(tp));
//            ConsumerHelp.doAssignTP(kafkaConsumer,100);

            ArrayList<Integer> emptyBatches = new ArrayList<>();
            try {
                for (int i = 0; i < 10000; i++) {
                    if(i%5==0){

                        System.out.println("当前批次"+i+", 进入"+maxPollInterval*2/1000+" 秒钟的休眠");
//                        Thread.sleep(maxPollInterval*2);
                        Thread.sleep(maxPollInterval);
                        System.out.println("休眠结束");

                        ConsumerRecords<String, String> poll = kafkaConsumer.poll(500);
                        if(poll.isEmpty()){
                            emptyBatches.add(i);
                        }
                        System.out.println(i+" 批次: 等待"+ maxPollInterval*2 +" 后 poll到数据量: "+poll.count()+"; \n截止当前发生的poll()为空的批次:"+emptyBatches.toString());
                    }else {
                        ConsumerRecords<String, String> poll = kafkaConsumer.poll(500);
                        if(poll.isEmpty()){
                            emptyBatches.add(i);
                        }
                    }
                    Thread.sleep(1000);
                }
            } catch (Exception e) {e.printStackTrace();}

//            consumer.close();
        }


        @Test
        public void testAssignOffsetOutOfRange(){
            Properties props = new Properties();
            props.put("key.deserializer", stringDeserializer);
            props.put("bootstrap.servers",brokerList);
            props.put("group.id", groupId);
            props.put("max.partition.fetch.bytes","1048576");
            props.put("value.deserializer", stringDeserializer);
            props.put("heartbeat.interval.ms", 5000);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            TopicPartition tp = new TopicPartition("testStringPerf", 0);
            consumer.assign(Collections.singleton(tp));


            consumer.seek(tp,10);
            for (int i = 0; i < 100; i++) {
                try{
                    ConsumerRecords<String, String> poll = consumer.poll(500);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            consumer.close();
        }
    }


    public static class TestSubscribeApi{
        String topic = "testStringPerf";

        @Test
        public void testSubscribeTypeSucceed(){
            try{
                KafkaConsumer<String, String> consumer = KafkaDevHelper.getKafkaConsumer(ImmutableMap.of(
//                        ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000",
//                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100",
//                    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollInterval),//10秒
//                    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "1000",
                        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,String.valueOf(1024*1024*10) ,//10M,1M=4034个,10M=4万个Record
                        ConsumerConfig.GROUP_ID_CONFIG, groupId
                ));
//                TopicPartition tp = new TopicPartition(topic, 1);
                List<TopicPartition> tps = ImmutableList.of(
//                        new TopicPartition(topic, 1),
//                        new TopicPartition(topic, 2),
                        new TopicPartition(topic, 3)
                );
                consumer.subscribe(Collections.singleton(topic));
//                consumer.seekToBeginning();

                ConsumerHelp.doConsumerInFixRate(consumer, true,10000,1000);
                consumer.close();

            }catch (Exception e){
                e.printStackTrace();
            }

        }


        @Test
        public void testSubscribeTypeRebalaceByTimeout(){
            int maxPollInterval = 10000;
            try{
                KafkaConsumer<String, String> consumer = KafkaDevHelper.getKafkaConsumer(ImmutableMap.of(
//                        ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000",
//                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100",
                    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollInterval),//10秒
//                    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "1000",
                        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,String.valueOf(1024*1024*10) ,//10M,1M=4034个,10M=4万个Record
                        ConsumerConfig.GROUP_ID_CONFIG, groupId
                ));
//                TopicPartition tp = new TopicPartition(topic, 1);
                List<TopicPartition> tps = ImmutableList.of(
//                        new TopicPartition(topic, 1),
//                        new TopicPartition(topic, 2),
                        new TopicPartition(topic, 3)
                );

                consumer.subscribe(Collections.singleton(topic), new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        System.out.println("准备Rebalance");
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        System.out.println("完成Rebalance");
                    }
                });


                ArrayList<Integer> emptyBatches = new ArrayList<>();
                try {
                    for (int i = 0; i < 10000; i++) {
                        if(i%5==0){
                            System.out.println("当前批次"+i+", 进入"+maxPollInterval*2/1000+" 秒钟的休眠");
                            Thread.sleep(maxPollInterval+2000);
//                            Thread.sleep(maxPollInterval);
                            System.out.println("休眠结束");
                            ConsumerRecords<String, String> poll = consumer.poll(500);
                            if(poll.isEmpty()){
                                emptyBatches.add(i);
                            }
                            System.out.println(i+" 批次: 等待"+ maxPollInterval*2 +" 后 poll到数据量: "+poll.count()+"; \n截止当前发生的poll()为空的批次:"+emptyBatches.toString());
                        }else {
                            ConsumerRecords<String, String> poll = consumer.poll(500);
                            if(poll.isEmpty()){
                                emptyBatches.add(i);
                            }
                        }
                        Thread.sleep(1000);
                    }
                } catch (Exception e) {e.printStackTrace();}
                consumer.close();

            }catch (Exception e){
                e.printStackTrace();
            }

        }



    }


    public static void doMain(String brokerList,String topic,String consumerGroup,int numRecords) {
        String[] array = KafkaDevHelper.parsedConsumerArags(brokerList,topic,consumerGroup,String.valueOf(numRecords));


    }


    public static void main(String[] args) {

        String[] array = KafkaDevHelper.parsedConsumerArags(args);


    }

}
