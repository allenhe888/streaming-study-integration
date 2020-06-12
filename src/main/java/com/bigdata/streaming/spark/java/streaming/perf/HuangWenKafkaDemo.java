package com.bigdata.streaming.spark.java.streaming.perf;

import com.bigdata.streaming.spark.java.SparkDevHelper;
import com.bigdata.streaming.spark.scala.streaming.kafkatest.KafkaSparkListener;
import com.bigdata.streaming.spark.scala.streaming.kafkatest.KafkaStreamingListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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

import java.util.*;

public class HuangWenKafkaDemo extends SparkDevHelper {

    @Test
    public void TestMain(){
        String argsString = "ldsver51:9092 testStringPerf sparkKafkaGroupId02 1000 10000 true true true 2 10485760";
        /*
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
        int batchDuration = 1000;
        if(args.length > 3) {
            batchDuration = Integer.parseInt(args[3]);
        }
        int maxRatePerPartition = 10000;
        if(args.length > 4) {
            maxRatePerPartition = Integer.parseInt(args[4]);
        }
        boolean isLocal = true;
        if(args.length>5){
            isLocal = Boolean.parseBoolean(args[5]);
        }
        boolean enableBackPressure = true;
        if(args.length>6){
            enableBackPressure = Boolean.parseBoolean(args[6]);
        }
        boolean enableAutoCommit = true;
        if(args.length>7){
            enableAutoCommit = Boolean.parseBoolean(args[7]);
        }
        int partitions = 16;
        if(args.length > 8) {
            partitions = Integer.parseInt(args[8]);
        }
        int fetchBytes = 1024*1024;
        if(args.length > 9) {
            fetchBytes = Integer.parseInt(args[9]);
        }
        // SparkStreaming 运行环境 local表示本地 如需在环境中需要删除
        SparkConf sparkConf = new SparkConf().setAppName(HuangWenKafkaDemo.class.getSimpleName());
        if(isLocal) {
            sparkConf.setMaster("local[1]");
        }
        // 设置序列化器为KryoSerializer。
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        // 反压机制
        sparkConf.set("spark.streaming.backpressure.enabled", String.valueOf(enableBackPressure));
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", String.valueOf(maxRatePerPartition));
        sparkConf.set("spark.streaming.dynamicAllocation.enabled", "false");

        // 添加监控和监控用参数
        sparkConf.set("spark.streaming.kafka.consumer.poll.ms", String.valueOf(1000 * 10));//提交poll超时时间,解决assertion failed: Failed to get records报错;
        sparkConf.set("spark.extraListeners", KafkaSparkListener.class.getName());
        sparkConf.set("spark.streaming.userdefine.batch.duration", String.valueOf(batchDuration));
        sparkConf.set("spark.streaming.userdefine.avg.start.batch", "20");
        sparkConf.set("spark.streaming.userdefine.record.size.per.job", String.valueOf((int)(batchDuration/1000.00 * maxRatePerPartition * partitions)));
        // 获取jssc 以及设置获取流的时间
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchDuration));
        // 加上Streaming Batch级别的监控
        jssc.addStreamingListener(new KafkaStreamingListener(jssc.ssc()));

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
        // 添加fetchMaxBytes参数
        kafkaParams.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, fetchBytes);

        Collection<String> topics = Arrays.asList(topic.split(","));

        // 获取流
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
        stream.foreachRDD( rdd -> {
            rdd.foreach( x -> {//没对fetch failed进行处理, qps高时很容易失败.
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
