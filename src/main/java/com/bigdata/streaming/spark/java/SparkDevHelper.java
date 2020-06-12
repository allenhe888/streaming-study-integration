package com.bigdata.streaming.spark.java;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.streaming.common.CommonHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SparkDevHelper extends CommonHelper{

    public static class Key{
        public static final String kafkaBootstrapServers = "kafka.bootstrap.servers";
        public static final String subscribe = "subscribe";
        public static final String SEPARATOR_EMTPY = " ";
        public static final String includeTimestamp = "includeTimestamp";

    }

    public SparkSession getLocalSparkSession(){
        SparkSession spark = SparkSession.builder()
                .appName("LocalSpark")
                .master("local[3]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        return spark;
    }

    public JavaStreamingContext createJavaStreamingContext(String master, int durationMs, Level logLevel) {
        SparkConf sparkConf = new SparkConf().setAppName(this.getClass().getSimpleName()).setMaster(master);
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(durationMs));
        jssc.sparkContext().setLogLevel(logLevel.toString());

        return jssc;
    }

    public SparkSession createSparkSession(String master, Level logLevel){
        SparkSession sparkSession = SparkSession.builder()
                .appName(this.getClass().getSimpleName())
                .master(master)
                .config("spark.eventLog.enabled",true)
                .config("spark.eventLog.dir","file:/E:/studyAndTest/eventLogDir")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel(logLevel.toString());
        return sparkSession;
    }

    public <K, V> JavaInputDStream<ConsumerRecord<K, V>> createKafkaDStream(JavaStreamingContext jssc, Map<String, Object> overrideParams, String... topics) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "ldsver51:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("group.id", "spark-streaming-groupId");

        if(overrideParams!=null && !overrideParams.isEmpty()){
            kafkaParams.putAll(overrideParams);
        }

        JavaInputDStream<ConsumerRecord<K, V>> directStream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<K, V>Subscribe(Arrays.asList(topics), kafkaParams)
        );
        return directStream;
    }


    public void processWordCountFunction(JavaInputDStream<ConsumerRecord<String, String>> directStream) {
        JavaDStream<ConsumerRecord<String, String>> transform = directStream.transform(
                new Function<JavaRDD<ConsumerRecord<String, String>>, JavaRDD<ConsumerRecord<String, String>>>() {
                    @Override
                    public JavaRDD<ConsumerRecord<String, String>> call(JavaRDD<ConsumerRecord<String, String>> data) throws Exception {
                        OffsetRange[] offsets = ((HasOffsetRanges) data.rdd()).offsetRanges();
                        String topic = offsets[0].topic();
                        for(OffsetRange offset: offsets){
                            System.out.println(offset);
                        }
                        return data;
                    }
                });

        JavaDStream<String> line = transform.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> data) throws Exception {
                if (data == null) {
                    return null;
                }
                return data.value();
            }
        });
        JavaDStream<String> words = line.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
//        JavaPairDStream<String, Integer> wordsCounts = words.mapToPair(w -> new Tuple2<>(w, 1));
//        JavaPairDStream<String, Integer> wordCountResult = wordsCounts.reduceByKey((k, v) -> k + v);
//        wordCountResult.print();

    }

    public void processWordCountWindowFunction(JavaInputDStream<ConsumerRecord<String, String>> directStream) {
        JavaDStream<ConsumerRecord<String, String>> transform = directStream.transform(
                new Function<JavaRDD<ConsumerRecord<String, String>>, JavaRDD<ConsumerRecord<String, String>>>() {
                    @Override
                    public JavaRDD<ConsumerRecord<String, String>> call(JavaRDD<ConsumerRecord<String, String>> data) throws Exception {
                        OffsetRange[] offsets = ((HasOffsetRanges) data.rdd()).offsetRanges();
                        String topic = offsets[0].topic();
                        for(OffsetRange offset: offsets){
                            System.out.println(offset);
                        }
                        return data;
                    }
                });

        JavaDStream<String> line = transform.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> data) throws Exception {
                if (data == null) {
                    return null;
                }
                return data.value();
            }
        });
        JavaDStream<String> words = line.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        // 这里开始调 Window函数
        JavaDStream<String> windowWords = words.window(Durations.seconds(20), Durations.seconds(10));
        JavaPairDStream<String, Integer> wordsPair = windowWords.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> reducedWords = wordsPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        reducedWords.print();

    }

    public void processWordCountReduceAndWindowFunction(JavaInputDStream<ConsumerRecord<String, String>> directStream) {
        JavaDStream<ConsumerRecord<String, String>> transform = directStream.transform(
                new Function<JavaRDD<ConsumerRecord<String, String>>, JavaRDD<ConsumerRecord<String, String>>>() {
                    @Override
                    public JavaRDD<ConsumerRecord<String, String>> call(JavaRDD<ConsumerRecord<String, String>> data) throws Exception {
                        OffsetRange[] offsets = ((HasOffsetRanges) data.rdd()).offsetRanges();
                        String topic = offsets[0].topic();
                        for(OffsetRange offset: offsets){
                            System.out.println(offset);
                        }
                        return data;
                    }
                });

        JavaDStream<String> line = transform.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> data) throws Exception {
                if (data == null) {
                    return null;
                }
                return data.value();
            }
        });
        JavaDStream<String> words = line.flatMap(x -> Arrays.asList(x.split(" ")).iterator());


//        JavaDStream<String> windowWords = words.window(Durations.seconds(20), Durations.seconds(10));
        JavaPairDStream<String, Integer> wordsPair = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        // 这里开始调 Window函数
        JavaPairDStream<String, Integer> reducedWords =wordsPair.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        },Durations.seconds(20), Durations.seconds(10));

        reducedWords.print();

    }

    public void processRecordWindowAggregatorFunction(JavaInputDStream<ConsumerRecord<String, String>> directStream) {

        JavaDStream<ConsumerRecord<String, String>> transform = directStream.transform(
                new Function<JavaRDD<ConsumerRecord<String, String>>, JavaRDD<ConsumerRecord<String, String>>>() {
                    @Override
                    public JavaRDD<ConsumerRecord<String, String>> call(JavaRDD<ConsumerRecord<String, String>> data) throws Exception {
                        int partitions = data.rdd().getNumPartitions();
                        OffsetRange[] offsets = ((HasOffsetRanges) data.rdd()).offsetRanges();
                        for(OffsetRange offset: offsets){
                            System.out.println("RDD.NumPartitions-"+partitions+" - "+offset);
                        }
                        return data;
                    }
                });
        JavaDStream<String> line = transform.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> data) throws Exception {
                if (data == null) {
                    return null;
                }
                return data.value();
            }
        });

        JavaDStream<JSONObject> json = line.map(x -> JSON.parseObject(x));


        JavaDStream<String> words = line.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
//        JavaPairDStream<String, Integer> wordsCounts = words.mapToPair(w -> new Tuple2<>(w, 1));
//        JavaPairDStream<String, Integer> wordCountResult = wordsCounts.reduceByKey((k, v) -> k + v);
//        wordCountResult.print();

    }


    class OffsetCommitCallbackImpl implements OffsetCommitCallback {
        final private boolean isPrintSucceed;
        public OffsetCommitCallbackImpl(final boolean isPrintSucceed) {
            this.isPrintSucceed = isPrintSucceed;
        }

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception ex) {
            if(null ==ex){
                if(isPrintSucceed){

                    System.out.println("offset提交成功: offsets="+offsets);
                }
            }else {
                System.err.println("offset提交失败! offsets"+offsets);
                ex.printStackTrace();
            }
        }
    }


    public void commitOffsetsForEachRDD(JavaInputDStream dStream,boolean isPrintSucceed){
        dStream.foreachRDD(new CommitOffsetImpl(dStream,isPrintSucceed));
    }

    public class CommitOffsetImpl implements VoidFunction<JavaRDD<ConsumerRecord<String, String>>> {
        private CanCommitOffsets canCommitOffsets;
        final private boolean isPrintSucceed;
        public CommitOffsetImpl(JavaInputDStream<ConsumerRecord<String, String>> kafkaStream, final boolean isPrintSucceed) {
            this.canCommitOffsets = (CanCommitOffsets) kafkaStream.inputDStream();
            this.isPrintSucceed = isPrintSucceed;
        }

        @Override
        public void call(JavaRDD<ConsumerRecord<String, String>> consumerRecordJavaRDD) throws Exception {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) consumerRecordJavaRDD.rdd()).offsetRanges();
            if (offsetRanges.length > 0) {
                canCommitOffsets.commitAsync(offsetRanges,new OffsetCommitCallbackImpl(isPrintSucceed));
            }
        }
    }


}
