package com.bigdata.streaming.spark.java.streaming.kafkaeos;

import com.bigdata.streaming.kafka.common.KafkaDevHelper;
import com.bigdata.streaming.spark.java.SparkDevHelper;
import com.google.common.collect.ImmutableMap;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

//import scala.Tuple2;

public final class JavaWordCount extends SparkDevHelper implements Serializable {

    public static void main(String[] args) throws InterruptedException {
        new JavaWordCount().testKafkaSparkStreamingInWordCount();

    }

    @Test
    public void testKafkaSparkStreamingInWordCount() throws InterruptedException {
        Namespace namespace = getArgsNamespaceByKVMapWithoutSeparator(ImmutableMap.of(
                "master", "local[3]",
                "brokerServers", "ldsver51:9092",
                "topics", "testKafkaApi",
                "consumerGroup", "stream-kafka-group-wordCount"
        ));

        ExecutorService es = KafkaDevHelper.ProducerHelp.runProducerThreadInRate(namespace.getString("topics"), 10, "ldsver51:9092", 10000,false);

        JavaStreamingContext jssc = createJavaStreamingContext(namespace.getString("master"),1000*5, Level.WARN);
        JavaInputDStream<ConsumerRecord<String, String>> directStream = createKafkaDStream(jssc,null,namespace.getString("topics").split(","));

//        processWordCountFunction(directStream);
//        processWordCountWindowFunction(directStream);
        processWordCountReduceAndWindowFunction(directStream);

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
        KafkaDevHelper.ProducerHelp.shutdownExecutorServer(es);
    }


    @Test
    public void testKafkaSparkStreamingInRecordWindowAggr() throws InterruptedException {
        Namespace namespace = getArgsNamespaceByKVMapWithoutSeparator(ImmutableMap.of(
                "master", "local[3]",
                "brokerServers", "ldsver51:9092",
                "topics", "testStringApi",
                "consumerGroup", "stream-kafka-group-recordWinAggr"
        ));
        String topic = namespace.getString("topics");

        ExecutorService es = KafkaDevHelper.ProducerHelp.runProducerThreadInRate(topic, 10, "ldsver51:9092", 10000,true);

        JavaStreamingContext jssc = createJavaStreamingContext(namespace.getString("master"),1000*5, Level.WARN);
        JavaInputDStream<ConsumerRecord<String, String>> directStream = createKafkaDStream(jssc,null,topic.split(","));

        processWordCountFunction(directStream);
        processRecordWindowAggregatorFunction(directStream);


        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
        KafkaDevHelper.ProducerHelp.shutdownExecutorServer(es);

    }

    @Test
    public void testKafkaStructuredStreamingInRecordWindowAggr() throws InterruptedException {
        Namespace namespace = getArgsNamespaceByKVMapWithoutSeparator(ImmutableMap.<String,String>builder()
                .put("master", "local[3]")
                .put("brokerServers", "ldsver51:9092")
                .put("topics", "testStringApi")
                .put("windowSize", "3")
                .put("slideSize", "1")
                .put("consumerGroup", "stream-kafka-group-recordWinAggr")
                .build());


        String topic = namespace.getString("topics");
        String brokerServers = namespace.getString("brokerServers");
        int windowSize = Integer.parseInt(namespace.getString("windowSize"));
        int slideSize = Integer.parseInt(namespace.getString("slideSize"));
        String windowDuration = windowSize + " secends";
        String slideDuration = slideSize + " secends";

        ExecutorService es = KafkaDevHelper.ProducerHelp.runProducerThreadInRate(topic, 10, "ldsver51:9092", 10000,true);



        JavaStreamingContext jssc = createJavaStreamingContext(namespace.getString("master"),1000*5, Level.WARN);
        SparkSession sparkSession = createSparkSession(namespace.getString("master"), Level.WARN);

        JavaInputDStream<ConsumerRecord<String, String>> directStream = createKafkaDStream(jssc,null,topic.split(","));
        Dataset<Row> lines = sparkSession.readStream()
                .format("kafka")
                .option(Key.kafkaBootstrapServers, brokerServers)
                .option(Key.subscribe, topic)
                .option(Key.includeTimestamp,true)
                .load();

        // 过滤和序列化?
        Encoder<Tuple2<String, Timestamp>> tuple = Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP());
        Dataset<Row> words = lines
                .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
                .flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) t -> {
                            List<Tuple2<String, Timestamp>> result = new ArrayList<>();
                            for (String word : t._1.split(" ")) {
                                boolean add = result.add(new Tuple2<>(word, t._2));
                            }
                            return result.iterator();
                        },
                        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())
                ).toDF("word", "timestamp");



        processWordCountFunction(directStream);
        processRecordWindowAggregatorFunction(directStream);


        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
        KafkaDevHelper.ProducerHelp.shutdownExecutorServer(es);

    }




    @Test
    public void testArgsParse(){
        Namespace namespace = getArgsNamespaceByKVMapWithoutSeparator(ImmutableMap.of(
                "master", "local[3]",
                "brokerServers", "ldsver51:9092",
                "topics", "testKafkaApi",
                "consumerGroup", "spark-streaming-groupId"
        ));





    }

}
