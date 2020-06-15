package com.bigdata.streaming.kafka.producer;

import com.bigdata.streaming.kafka.common.KafkaDevHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import javafx.util.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;

public class ProducerApiDemo extends KafkaDevHelper.ProducerHelp {
    public int numRecords = 10000 * 100;


    @Test
    public void testProducerByByteArray(){
        KafkaProducer<String, byte[]> producer = getProducer(ImmutableMap.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName()
        ));
        testProducerRun(producer,topic,numRecords, ImmutableList.of(
                new Pair<>("key1",("value--"+ UUID.randomUUID().toString()).getBytes()),
                new Pair<>("key2",("value--"+ UUID.randomUUID().toString()).getBytes())
        ));
    }

    @Test
    public void testProducerByString(){
        KafkaProducer<String, String> producer = getProducer(null);
        testProducerRun(producer,topic,numRecords, ImmutableList.of(
                new Pair<>("key1","value--"+ UUID.randomUUID().toString()),
                new Pair<>("key2","value--"+ UUID.randomUUID().toString())
        ));
    }

    @Test
    public void testProducerByJSON(){
        KafkaProducer<String, String> producer = getProducer(null);
        topic = "testStringPerf";
        testProducerRun(producer,topic,10000 * 500, ImmutableList.of(
                new Pair<>("key1", getRecordString()),
                new Pair<>("key2", getRecordString()),
                new Pair<>("key3", getRecordString()),
                new Pair<>("key4", getRecordString()),
                new Pair<>("key5", getRecordString())
        ));
    }

//    public void sendWithKeys(String[] args) throws Exception {
//
//        Namespace namespace = KafkaDevHelper.getNamespaceByArgsParser(args,ImmutableMap.<String, String>builder()
//                .put(CommKey.bootstrapServers, bootstrapServers)
//                .put(CommKey.topic, topic)
//                .put(CommKey.numRecords, "100000")
//                .build());
//
//
//        KafkaProducer<String, String> producer = getProducer(ImmutableMap.of(
//                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, namespace.getString(CommKey.bootstrapServers),
//                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSerializer,
//                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, stringSerializer
//        ));
//
//        List<String> templateMsg = new ArrayList<>();
//        Map<String,String> templateData = ImmutableMap.of();
//        templateData = parsetInputKeyValueFromJson();
//
//        testProducerRun(producer,namespace.getString(CommKey.bootstrapServers),namespace.getInt(CommKey.numRecords), ImmutableList.of(
//                new Pair<>("key1", getRecordString()),
//                new Pair<>("key2", getRecordString()),
//                new Pair<>("key3", getRecordString()),
//                new Pair<>("key4", getRecordString()),
//                new Pair<>("key5", getRecordString())
//        ));
//
//
//
//
//    }

    private Map<String, String> parsetInputKeyValueFromJson() {

        return null;
    }


    public static void doMain(String bootstrapServers,String topic,int numRecords) throws Exception {
        KafkaProducer<String, String> producer = getProducer(ImmutableMap.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSerializer,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, stringSerializer
        ));
        testProducerRun(producer,topic,numRecords, ImmutableList.of(
                new Pair<>("key1", getRecordString()),
                new Pair<>("key2", getRecordString()),
                new Pair<>("key3", getRecordString()),
                new Pair<>("key4", getRecordString()),
                new Pair<>("key5", getRecordString())
        ));
    }


}
