package com.bigdata.streaming.spark.java.streaming.perf;

import com.bigdata.streaming.common.CommKey;
import com.bigdata.streaming.spark.java.SparkDevHelper;
import com.google.common.collect.ImmutableMap;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.deploy.yarn.ApplicationMaster;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SparkKafkaPerfDemo extends SparkDevHelper {

    public void testViewApplicationMaster(){
        ApplicationMaster.main(null);

    }

    public void testKafkaDStreamSimpleCollectImpl(String[] args){
        Namespace namespace = getNamespaceByArgsParser(args, ImmutableMap.<String, String>builder()
                .put(CommKey.bootstrapServers, "ldsver51:9092")
                .put(CommKey.topic, "HJQ_testKafkaPerPartition")
                .put(CommKey.KeyDeserializer, StringDeserializer.class.getName())
                .put(CommKey.ValueDeserializer, StringDeserializer.class.getName())
                .put(CommKey.maxRatePerPartition, "500")
                .put(CommKey.kafkaPollMaxRetries, "5")
                .put(CommKey.enableAutoCommit, "false")
                .put(CommKey.groupId, "java-consumer-kafkaDStream-01")

                .put(CommKey.commitOffsetsForeach, "true")
                .put(CommKey.isPrintSucceed, "true")

                .put(CommKey.master, "local[3]")
                .put(CommKey.deployMode, "client")
                .put(CommKey.batchDuration, "3000")
                .build());

        SparkConf sparkConf = new SparkConf()
                .setMaster(namespace.getString(CommKey.master))
                .setIfMissing("spark.submit.deployMode",namespace.getString(CommKey.deployMode))
                .setAppName(this.getClass().getSimpleName());
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", namespace.getString(CommKey.maxRatePerPartition));
        sparkConf.set("spark.streaming.kafka.consumer.poll.max.retries", namespace.getString(CommKey.kafkaPollMaxRetries));

        int durationMs = Integer.parseInt(namespace.getString(CommKey.batchDuration));
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(durationMs));
        final String topic = namespace.getString(CommKey.topic);
        final String bootstrapServers = namespace.getString(CommKey.bootstrapServers);

        final Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", bootstrapServers);
        kafkaParams.put("key.deserializer", namespace.getString(CommKey.KeyDeserializer));
        kafkaParams.put("value.deserializer", namespace.getString(CommKey.KeyDeserializer));
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit",namespace.getString(CommKey.enableAutoCommit));
        kafkaParams.put("group.id", namespace.getString(CommKey.groupId));

        JavaInputDStream<ConsumerRecord<String, String>> kafkaDStream = KafkaUtils.createDirectStream(
                jssc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String,String>Subscribe(Arrays.asList(topic), kafkaParams));
        boolean isCommitOffset = Boolean.parseBoolean(namespace.getString(CommKey.commitOffsetsForeach));
        if(isCommitOffset){
            commitOffsetsForEachRDD(kafkaDStream,Boolean.parseBoolean(namespace.getString(CommKey.commitOffsetsForeach)));
            kafkaDStream.count();
        }else {
            kafkaDStream.foreachRDD(rdd->{
                rdd.map(record -> record.value()).collect();
            });
        }
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testKafkaDStreamWith4Args(){
        String argsString = "ldsver51:9092 HJQ_testKafkaPerPartition sparkTestGroupId_01 1000 20000 false 3000";

        testKafkaDStreamWith4ArgsImpl(argsString.split(CommKey.EMPTY_STRING),true);
    }

    public void testKafkaDStreamWith4ArgsImpl(String[] args,boolean isLocal){
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

        int durationMs = 1000;
        if(args.length>3){
            try{
                durationMs =  Integer.parseInt(args[3]);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        int maxRatePerPartition = 20000;
        if(args.length>4){
            try{
                maxRatePerPartition= Integer.parseInt(args[4]);
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        boolean isCommitOffset = false;
        if(args.length>5){
            try{
                isCommitOffset= Boolean.parseBoolean(args[5]);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        int reportInterval = 5000;
        if(args.length>6){
            try{
                reportInterval= Integer.parseInt(args[6]);
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        SparkConf sparkConf = new SparkConf().setAppName(this.getClass().getSimpleName());
        if(isLocal){
            sparkConf.setMaster("local[2]");
        }
        sparkConf.set("spark.streaming.kafka.consumer.poll.max.retries", "5");
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", String.valueOf(maxRatePerPartition));

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(durationMs));
        final Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", bootstrapServers);
        kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit","false");
        kafkaParams.put("group.id", "java-consumer-kafkaDStream");

        JavaInputDStream<ConsumerRecord<String, String>> kafkaDStream = KafkaUtils.createDirectStream(
                jssc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String,String>Subscribe(Arrays.asList(topic), kafkaParams));

        final int reportIntMillis = reportInterval;
        final AtomicLong totalRecords = new AtomicLong(0);
        final AtomicLong lastReportTime = new AtomicLong(System.currentTimeMillis());
        final AtomicLong lastReportNum = new AtomicLong(0);
        final SimpleDateFormat format = new SimpleDateFormat("HH:mm分 sss.SSS");
        if(isCommitOffset){
            commitOffsetsForEachRDD(kafkaDStream,false);
            kafkaDStream.count();
        }else {
            kafkaDStream.foreachRDD(rdd->{
                JavaRDD<Integer> partitionCount = rdd.mapPartitions(it -> {
                    AtomicInteger partitionCounter = new AtomicInteger(0);
                    while (it.hasNext()) {
                        partitionCounter.incrementAndGet();
                    }
                    return Arrays.asList(partitionCounter.get()).iterator();
                });
                AtomicInteger sum = new AtomicInteger(0);
                List<Integer> result = partitionCount.collect();
                result.forEach(ele->sum.getAndAdd(ele));
                long count = sum.get();
//                long count = rdd.count();
                long curTotal = totalRecords.addAndGet(count);
                long curTime = System.currentTimeMillis();
                if((curTime - lastReportTime.get() > reportIntMillis) ){
                    long delta = curTotal - lastReportNum.get();
                    double qps = BigDecimal.valueOf(delta).divide(BigDecimal.valueOf(curTime - lastReportTime.get()),3, RoundingMode.HALF_UP).doubleValue();
                    double qps2 = BigDecimal.valueOf(qps).divide(new BigDecimal("10"),3, RoundingMode.HALF_UP).doubleValue();
                    System.out.println(format.format(new Date(curTime))+":\t\t "+reportIntMillis+"ms间隔内处理:"+delta+"个 ;\t 期间QPS: "+qps+" Record/ms ( "+(qps2)+" 万/秒);\t 累计各批处理总数:"+curTotal);
                    lastReportTime.set(curTime);
                    lastReportNum.set(curTotal);
                }
            });
        }
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    @Test
    public void testKafkaDStreamSimpleCollect(){
        String argsString = "--bootstrapServers ldsver51:9092 --topic HJQ_testKafkaPerPartition --maxRatePerPartition 1000 "+
                "--kafkaPollMaxRetries 8 --enableAutoCommit false --groupId kafkaDStream-consumerGroup-02 "+
                "--commitOffsetsForeach true --duration 4000 --commitOffsetsForeach false ";

        testKafkaDStreamSimpleCollectImpl(argsString.split(CommKey.EMPTY_STRING));
    }



    public static void main(String[] args) {
        String argsStr = "";
        /*
        --bootstrapServers ldsver51:9092 --topic HJQ_testKafkaPerPartition --maxRatePerPartition 1000 --kafkaPollMaxRetries 8 --enableAutoCommit false --groupId kafkaDStream-consumerGroup-02 --commitOffsetsForeach true --duration 4000 --commitOffsetsForeach false
         */
//        new SparkKafkaPerfDemo().testKafkaDStreamSimpleCollectImpl(args);

        /*
        ldsver51:9092 HJQ_testKafkaPerPartition sparkTestGroupId 1000 20000 false
         */
        new SparkKafkaPerfDemo().testKafkaDStreamWith4ArgsImpl(args,false);

    }


}
