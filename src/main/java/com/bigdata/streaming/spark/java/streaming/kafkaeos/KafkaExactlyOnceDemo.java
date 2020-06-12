package com.bigdata.streaming.spark.java.streaming.kafkaeos;

import com.bigdata.streaming.spark.java.SparkDevHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class KafkaExactlyOnceDemo extends SparkDevHelper {

    public String kafakServer = "ldsver51:9092";
    public String topic = "testKafakApi";
    public String consumerGroup = "consumerGroup-spark";

    @Test
    public void testEOSKafkaConsumer(){
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafakServer);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", consumerGroup);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList(topic);

        JavaStreamingContext streamingContext = new JavaStreamingContext(getLocalSparkSession().sparkContext().conf(), new Duration(2000));




        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));



        OffsetRange[] offsetRanges = {
                // topic, partition, inclusive starting offset, exclusive ending offset
                OffsetRange.create("test", 0, 0, 100),
                OffsetRange.create("test", 1, 0, 100)
        };

        JavaRDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(
                streamingContext.sparkContext(),
                kafkaParams,
                offsetRanges,
                LocationStrategies.PreferConsistent()
        );


    }



    @Test
    public void testKafkaConsumerReadOffsetsFromDB(){
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafakServer);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", consumerGroup);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList(topic);

        JavaStreamingContext streamingContext = new JavaStreamingContext(getLocalSparkSession().sparkContext().conf(), new Duration(2000));

        // begin from the the offsets committed to the database
        Map<TopicPartition, Long> fromOffsets = new HashMap<>();
        fromOffsets = getOffsetsFromDB();
//        for (resultSet : selectOffsetsFromYourDatabase)
//            fromOffsets.put(new TopicPartition(resultSet.string("topic"), resultSet.int("partition")), resultSet.long("offset"));
//        }

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Assign(fromOffsets.keySet(), kafkaParams, fromOffsets)
        );

        stream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
//            Object results = yourCalculation(rdd);
            // begin your transaction

            // update results
            // update offsets where the end of existing offsets matches the beginning of this batch of offsets
            // assert that offsets were updated correctly

            // end your transaction
        });


    }

    private Map<TopicPartition, Long> getOffsetsFromDB() {
        return null;
    }

}
