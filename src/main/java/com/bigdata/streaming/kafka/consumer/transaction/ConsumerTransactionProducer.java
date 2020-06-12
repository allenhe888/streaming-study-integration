package com.bigdata.streaming.kafka.consumer.transaction;

import com.bigdata.streaming.kafka.common.KafkaDevHelper;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Future;

public class ConsumerTransactionProducer extends KafkaDevHelper.ProducerHelp {

    @Test
    public void testTransactionProducer(){
        String topic = "testKafkaApi";
        String testAsynTopic = "testAsynTopic";
        KafkaProducer<String, String> producer = getTransactionProducer(ImmutableMap.<String,Object>builder()
                .put(ProducerConfig.RETRIES_CONFIG,3)
                .put(ProducerConfig.BATCH_SIZE_CONFIG,100)
                .put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432)
                .put(ProducerConfig.LINGER_MS_CONFIG,1000)// Linge:逗留时间,缓存同一批次最长逗留时间;
                .put(ProducerConfig.CLIENT_ID_CONFIG,"clientId-tx-producer")
                .put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"transactionId-producer")
                .put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true)// Idempotence:幂等性;
                .build()
        );

        producer.initTransactions();

        try {
            Thread.sleep(2000L);// 等待 事务完成初始化启动;

            producer.beginTransaction();// 开启事务;
            producer.send(new ProducerRecord<>(topic,"key-syn","value-01"));
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(testAsynTopic,"key-asyn", "value-01"));

            producer.commitTransaction();

        } catch (InterruptedException e) {
            e.printStackTrace();
            producer.abortTransaction();
        }finally {
            producer.close();
        }

    }

    @Test
    public void testProducerUtil(){
        String testConsumerTopic = "testConsumerTopic";
        String testProducerTopic = "testProducerTopic";
        String consumerGroup = "condumerGroup-common";
        int numRecords = 500;
        KafkaDevHelper.ProducerHelp.sendToTopicWithNumRecords(testConsumerTopic,numRecords,servers);

        KafkaProducer<String, String> producer = KafkaDevHelper.ProducerHelp.createTransactionalProducer(transactionalId, servers);
        KafkaConsumer<String, String> consumer = KafkaDevHelper.ConsumerHelp.createReadCommittedConsumer(consumerGroup, numRecords/4, null);
        consumer.subscribe(Collections.singleton(testConsumerTopic));
        producer.initTransactions();

        boolean shouldCommit = false;
        int recordsProcessed = 0;
        try{
            while (recordsProcessed < numRecords){
                List<ConsumerRecord<String, String>> records = KafkaDevHelper.ConsumerHelp.doConsumer(consumer, Math.min(10, numRecords - recordsProcessed), 3000, true);

                if(records!=null && !records.isEmpty()){
                    producer.beginTransaction();
                    shouldCommit = !shouldCommit;
                    for(ConsumerRecord<String, String> rc: records){
                        producer.send(KafkaDevHelper.ProducerHelp.generateStringProducerRecord(testProducerTopic, rc.key(), rc.value(), shouldCommit));
                    }

                    Map<TopicPartition, OffsetAndMetadata> offsets =KafkaDevHelper.ConsumerHelp.getConsumerOffsetPosition(consumer);
                    producer.sendOffsetsToTransaction(offsets, testConsumerTopic);

                    // 随机提交
                    if(shouldCommit){
                        producer.commitTransaction();
                        recordsProcessed += records.size();
                    }else {
                        producer.abortTransaction();
                        System.out.println("abort Transaction, offsets="+offsets+",\t records.size="+records.size());
                        KafkaDevHelper.ConsumerHelp.resetToCommittedPositions(consumer);
                    }

                }

            }

        }finally {
            consumer.close();
            producer.close();
        }

    }

    @Test
    public void testConsumerTransactionProducer(){
        String testConsumerTopic = "testConsumerTopic";
        String testProducerTopic = "testProducerTopic";
        String consumerGroup = "testConsumerGroup";
        int numRecords = 500;

        KafkaDevHelper.ProducerHelp.sendToTopicWithNumRecords(testConsumerTopic,numRecords,servers);

        KafkaProducer<String, String> producer = getTransactionProducer(ImmutableMap.<String,Object>builder()
                .put(ProducerConfig.LINGER_MS_CONFIG,1000)// Linge:逗留时间,缓存同一批次最长逗留时间;
                .put(ProducerConfig.CLIENT_ID_CONFIG,"clientId-consumer-tx-producer")
                .put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"transactionId-producer")
                .build()
        );

        KafkaConsumer<String, String> consumer = KafkaDevHelper.getKafkaConsumer(ImmutableMap.of(
                ConsumerConfig.GROUP_ID_CONFIG,consumerGroup
        ));
        producer.initTransactions();
        consumer.subscribe(Collections.singleton(testConsumerTopic));
        Set<TopicPartition> topicPartitions = KafkaDevHelper.ConsumerHelp.doAssignTP(consumer,100);
        HashMap<Integer, TopicPartition> thisTopicTP = new HashMap<>();
        topicPartitions.forEach(tp ->{thisTopicTP.put(tp.partition(),tp);});

        for(int i=0;i<100;i++){
            producer.beginTransaction();
            ConsumerRecords<String, String> poll = consumer.poll(100);
            try{
                HashMap<TopicPartition, OffsetAndMetadata> toCommits = new HashMap<>();
                for(ConsumerRecord<String,String> record: poll){
                    System.out.printf("record: offset=%d,  key=%s,\t value=%s \n",record.offset(), record.key(), record.value());

                    TopicPartition topicPartition = thisTopicTP.get(record.partition());
                    toCommits.put(topicPartition,new OffsetAndMetadata(record.offset()));

                    int perview=0;
                    try {
                        perview = Integer.parseInt(record.value());
                    }catch (Exception e){

                    }
                    int increase = perview + 10;
                    producer.send(new ProducerRecord<>(testProducerTopic,String.valueOf(perview), String.valueOf(increase)));

                    /*sendOffsetsToTransaction()
                        向事务协调器器发送一个AddOffesetCommitsToTxnRequests：
                        还会向消费者协调器Cosumer Corrdinator发送一个TxnOffsetCommitRequest，在主题_consumer_offsets中保存消费者的偏移量信息
                     */
                    producer.sendOffsetsToTransaction(toCommits,consumerGroup);//为消费者提供在事务内的位移提交的操作

                    producer.commitTransaction();
                }

                Thread.sleep(500);
                System.out.println("循环一次");
            }catch (Exception e){
                e.printStackTrace();
                producer.abortTransaction();
            }
        }

        producer.close();

    }


}
