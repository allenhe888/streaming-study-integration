package com.bigdata.streaming.spark.java.streaming.perf;

import com.bigdata.streaming.spark.java.SparkDevHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class HuangWenKafkaDemo extends SparkDevHelper {

    static final Pattern SPACE = Pattern.compile(" ");

    @Test
    public void TestMain(){
        String argsString = "ldsver51:9092 HJQ_testKafkaPerPartition sparkKafka_IdeaConsumer02 2 10000 true false false";
        /*
                                                                                                       local[2]   自动提交
                                                                                                             反压
         */
        HuangWenKafkaDemo.main(argsString.split(" "));
    }

    public static void main(String[] args) {
        String bootstrapServers = "ldsver51:9092";
        if(args.length>0){
            bootstrapServers= args[0];
        }
        String topic = "HJQ_testKafkaPerPartition";
        if(args.length>1){
            topic= args[1];
        }
        String groupId = "sparkTestGroupId";
        if(args.length>2){
            groupId= args[2];
        }else {
            groupId= groupId+"-"+new Random().nextInt(100);
        }

        int batchTime = 1;
        if(args.length > 3) {
            batchTime = Integer.parseInt(args[3]);
        }
        String rate = "10000";
        if(args.length > 4) {
            rate = args[4];
        }
        boolean isLocal = true;
        if(args.length>5){
            isLocal = Boolean.parseBoolean(args[5]);
        }
        boolean enableBackPressure = false;
        if(args.length>6){
            enableBackPressure = Boolean.parseBoolean(args[6]);
        }
        boolean enableAutoCommit = false;
        if(args.length>7){
            enableAutoCommit = Boolean.parseBoolean(args[7]);
        }


        // SparkStreaming 运行环境 local表示本地 如需在环境中需要删除
        SparkConf sparkConf = new SparkConf().setAppName(HuangWenKafkaDemo.class.getSimpleName());
        if(isLocal) {
            sparkConf.setMaster("local[2]");
        }

        // 设置序列化器为KryoSerializer。
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        // 反压机制
        sparkConf.set("spark.streaming.backpressure.enabled", String.valueOf(enableBackPressure));
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", rate);
        sparkConf.set("spark.streaming.dynamicAllocation.enabled", "false");
        // 获取jssc 以及设置获取流的时间
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchTime));

        // Kafka 参数配置
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        // kafka机器地址
        kafkaParams.put("bootstrap.servers", bootstrapServers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        // 设置消费者组
        kafkaParams.put("group.id", groupId);
        // 设置读取offset的规则 latest（默认） 为最新的offset earliest为从头开始
        kafkaParams.put("auto.offset.reset", "earliest");
        // 设置自动提交offset 注意：自动提交有数据丢失的可能
        kafkaParams.put("enable.auto.commit", enableAutoCommit);

        Collection<String> topics = Arrays.asList(topic.split(","));

        // 获取流
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        final int reportIntMillis = 5000;
        final AtomicLong totalRecords = new AtomicLong(0);
        final AtomicLong lastReportTime = new AtomicLong(System.currentTimeMillis());
        final AtomicLong lastReportNum = new AtomicLong(0);
        final SimpleDateFormat format = new SimpleDateFormat("HH:mm分 sss.SSS");
        stream.foreachRDD( rdd -> {
            rdd.foreach( x -> {
//	    		  System.out.println("x: " + x);
            });

        });
        try {
            // 启动
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
