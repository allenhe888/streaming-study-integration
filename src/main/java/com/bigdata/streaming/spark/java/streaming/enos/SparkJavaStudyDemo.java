/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bigdata.streaming.spark.java.streaming.enos;

import com.bigdata.streaming.common.CommKey;
import com.bigdata.streaming.common.CommonHelper;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import javafx.util.Pair;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.junit.Test;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class SparkJavaStudyDemo extends CommonHelper implements Serializable {



  private SparkConf getSparkConf(){
    SparkConf sparkConf = new SparkConf().setMaster("local[3]").setAppName(this.getClass().getSimpleName());
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", String.valueOf(500));
    sparkConf.set("spark.streaming.kafka.consumer.poll.max.retries", "5");
//    sparkConf.set("spark.streaming.backpressure.enabled", "true");
    return sparkConf;
  }



  public class ClusterSourceOffsetJson {
    private final String offset;
    private final String version;

    @JsonCreator
    public ClusterSourceOffsetJson(
            @JsonProperty("offset") String offset,
            @JsonProperty("version") String version
    ) {
      this.offset = offset;
      this.version = version;
    }

    public String getOffset() {
      return offset;
    }
    public String getVersion() {
      return version;
    }
  }


  ObjectMapper objectMapper = new ObjectMapper();
  private Map<Integer, Long> readClusterOffsetFile(InputStream fileInputStream, int numberOfPartitions) throws IOException{


    ClusterSourceOffsetJson clusterSourceOffsetJson = objectMapper.readValue(fileInputStream, ClusterSourceOffsetJson.class);
    String lastSourceOffset = clusterSourceOffsetJson.getOffset();
    if(lastSourceOffset !=null && !lastSourceOffset.isEmpty()){
      return deserializeKafkaPartitionOffset(lastSourceOffset, numberOfPartitions);
    } else {
      throw new IOException("Partition Offset Cannot be empty");
    }
  }

  private Map<Integer, Long> deserializeKafkaPartitionOffset(String partitionOffset, int numberOfPartitions) throws IOException {
    Map<Integer, Long> partitionToOffsetMap = new HashMap<>();
    int greatestPartitionFromOffset = -1;

    if (partitionOffset!=null && !partitionOffset.isEmpty()) {
      Map<String, Object> deserializedPartitionOffset = objectMapper.readValue(partitionOffset, Map.class);
      if (deserializedPartitionOffset.isEmpty()) {
        throw new IOException("Partition Offset cannot be empty");
      }

      for (Map.Entry<String, Object> partitionOffsetEntry : deserializedPartitionOffset.entrySet()) {
        int partition = Integer.parseInt(partitionOffsetEntry.getKey());
        Long offset = Long.parseLong(partitionOffsetEntry.getValue().toString());
        partitionToOffsetMap.put(partition, offset);
        greatestPartitionFromOffset = (partition > greatestPartitionFromOffset) ? partition : greatestPartitionFromOffset;
      }
    }

    //Basically add new partitions with offset 0.
    for (int partition = greatestPartitionFromOffset + 1; partition < numberOfPartitions; partition++) {
      partitionToOffsetMap.put(partition, 0L);
    }


    return partitionToOffsetMap;
  }


  String offsetFilePath = "E:\\studyAndTest\\sparkTemp\\kafka-offsets-save\\offsets.json";
  private Map<Integer, Long> readOffsets(int numberOfPartitions){
    Map<Integer, Long> offsets;

    Path currentCheckPointFilePath = new Path(offsetFilePath);
    File file = new File(currentCheckPointFilePath.toUri());

    InputStream io=null;
    try {
      io = new FileInputStream(file);
      if (file.exists()) {
        offsets = readClusterOffsetFile(io, numberOfPartitions);
//        writeOffsetsToMainOffsetFile(offsets);
      } else {
        offsets = readClusterOffsetFile(io, numberOfPartitions);
      }

      return offsets;
    } catch (IOException ex) {
      throw new RuntimeException("Error reading offset from hdfs path: {}. Reason: {}");
    }finally {
      try {
        io.close();
      } catch (IOException e) {
        e.printStackTrace();
        io=null;
      }
    }
  }

  private Map<TopicPartition, Long> getOffsetForDStream(String topic, int numberOfPartitions) {
    Map<TopicPartition, Long> offsetForDStream = new HashMap<>();
    Map<Integer, Long> partitionsToOffset = readOffsets(numberOfPartitions);
    for (Map.Entry<Integer, Long> partitionAndOffset : partitionsToOffset.entrySet()) {
      offsetForDStream.put(new TopicPartition(topic, partitionAndOffset.getKey()), partitionAndOffset.getValue());
    }
    return offsetForDStream;
  }


  private void saveOffsets(Map<Integer, Long> partitionToOffset) {
    try {
      writeOffsetsToMainOffsetFile(partitionToOffset);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private int createConsumerAndPartitionForTopic(String topic) {
    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,"ldsver51:9092");
    props.put("groupId.id","sdcTopicMetadataClient-myDev");
    props.put("enable.auto.commit", "false");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
    List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);


    partitionInfos.forEach(info -> {
      TopicPartition topicPartition = new TopicPartition(info.topic(), info.partition());
    });
    kafkaConsumer.close();
    int partitionCount = 0;
    if(partitionInfos!=null){
      partitionCount = partitionInfos.size();
    }
    System.out.println("获得到的该topic可用分区数为: "+partitionCount);
    return partitionCount;
  }

  private Map<TopicPartition, Long> getPartitionsWithStartOffset(String topic, long startOffset) {
    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,"ldsver51:9092");
    props.put("groupId.id","sdcTopicMetadataClient-myDev");
    props.put("enable.auto.commit", "false");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
    List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);

    Map<TopicPartition, Long> partitonOffsets = new HashMap<>();
    partitionInfos.forEach(info -> {
      TopicPartition topicPartition = new TopicPartition(info.topic(), info.partition());
      partitonOffsets.put(topicPartition,startOffset);
    });
    kafkaConsumer.close();

    return partitonOffsets;
  }




  @Test
  public void testKafkaRDD(){
    JavaSparkContext sc = new JavaSparkContext(getSparkConf());
//    final String topic1 = "testStringPerf";
    final String topic1 = "testKafkaApi";
    final String bootstrapServers = "ldsver51:9092";

    final Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", bootstrapServers);
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("auto.offset.reset", "earliest");
    kafkaParams.put("enable.auto.commit", "false");
    kafkaParams.put("max.poll.records", "100");
    kafkaParams.put("group.id", "java-consumer-03-kafkaRDD");

    int untilOffset = 2000;
    OffsetRange[] offsetRanges = {
            OffsetRange.create(topic1, 0, 0, untilOffset),
            OffsetRange.create(topic1, 1, 0, untilOffset)
//            OffsetRange.create(topic1, 2, 0, untilOffset)
    };
    JavaRDD<ConsumerRecord<String, String>> kafkaRDD = KafkaUtils.<String, String>createRDD(sc, kafkaParams, offsetRanges, LocationStrategies.PreferConsistent());
    JavaRDD<String> mapRDD = kafkaRDD.map(new MyMapFunc());


    JavaRDD<String> mapPartRDD = kafkaRDD.mapPartitions(new RecordMapPartFunc());

//    JavaRDD<Integer> integerJavaRDD = mapRDD.mapPartitions(it -> {
//      int sum = 0;
//      while (it.hasNext()){
//      }
//      return Collections.singleton(sum).iterator();
//    });

//    mapRDD.count();
    while (true){
      mapPartRDD.foreach((it)->{
        System.err.println(it);
      });

      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }


  }


  private Pair<JavaStreamingContext, JavaInputDStream<ConsumerRecord<String, String>>> getKafkaDStream(){
    JavaStreamingContext jssc = new JavaStreamingContext(getSparkConf(), Durations.milliseconds(1000* 10));
    final String topic = "HJQ_testKafkaPerPartition";
//    final String topic = "testKafkaApi";
    final String bootstrapServers = "ldsver51:9092";
    final Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", bootstrapServers);
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("auto.offset.reset", "earliest");
    kafkaParams.put("enable.auto.commit", "false");
//    kafkaParams.put("max.poll.records", "100");
    kafkaParams.put("group.id", "java-consumer-05-kafkaRDD");


    JavaInputDStream<ConsumerRecord<String, String>> kafkaDStream;
    if (new File(offsetFilePath).exists()) {
      // sdc中 numberOfPartitions= Utils.getNumberOfPartitions(getProperties()),
      // 从 props中读取 partitionCount 对应的值为 numberOfPartitions;
      /**
       * SDC-Cluster如何设置 KafkaRDD的分区数量
       ClusterRunner.doStart() -> ClusterRunner.getClusterSourceInfo() -> clusterSource.getConfigsToShip()
       -> ClusterKafkaSource.getConfigsToShip(){
       new ImmutableMap.Builder<String, String>().put("partitionCount", String.valueOf(getParallelism())){// BaseKafakSource.getParallelism()
       //
       originParallelism = kafkaValidationUtil[KafkaValidationUtil09].getPartitionCount(conf.metadataBrokerList,conf.topic,new HashMap<String, Object>(conf.kafkaConsumerConfigs),3,1000);{
       KafkaValidationUtil09.getPartitionCount(String metadataBrokerList,String topic){
       KafkaConsumer<String, String> kafkaConsumer = createTopicMetadataClient(metadataBrokerList, kafkaClientConfigs);
       List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topic);
       partitionCount = partitionInfoList.size();
       }
       }
       }
       }
       *
       */

//      int numberOfPartitions = createConsumerAndPartitionForTopic(topic);
//      Map<TopicPartition, Long> fromOffsets = getOffsetForDStream(topic, numberOfPartitions);

      Map<TopicPartition, Long> fromOffsets = getPartitionsWithStartOffset(topic, 10);

      kafkaDStream =
              KafkaUtils.createDirectStream(
                      jssc,
                      LocationStrategies.PreferConsistent(),
                      ConsumerStrategies.<String,String>Assign(new ArrayList<TopicPartition>(fromOffsets.keySet()), kafkaParams, fromOffsets)
              );
    } else {
      kafkaDStream  = KafkaUtils.createDirectStream(
              jssc,
              LocationStrategies.PreferConsistent(),
              ConsumerStrategies.<String,String>Subscribe(Arrays.asList(topic), kafkaParams)
      );

    }


    Pair<JavaStreamingContext, JavaInputDStream<ConsumerRecord<String, String>>> pair = new Pair<>(jssc, kafkaDStream);
    return pair;

  }



  public void testKafkaDStreamSimpleCollectImpl(String[] args){

    Namespace namespace = getNamespaceByArgsParser(args, ImmutableMap.<String, String>builder()
            .put(CommKey.bootstrapServers, "ldsver51:9092")
            .put(CommKey.topic, "testStringPerf")
            .put(CommKey.KeyDeserializer, StringDeserializer.class.getName())
            .put(CommKey.ValueDeserializer, StringDeserializer.class.getName())
            .put(CommKey.maxRatePerPartition, "500")
            .put(CommKey.kafkaPollMaxRetries, "5")
            .put(CommKey.enableAutoCommit, "false")
            .put(CommKey.groupId, "java-consumer-kafkaDStream-01")

            .put(CommKey.batchDuration, "3000")
            .build());

    SparkConf sparkConf = new SparkConf().setAppName(this.getClass().getSimpleName());
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
            jssc,LocationStrategies.PreferConsistent(),
            ConsumerStrategies.<String,String>Subscribe(Arrays.asList(topic), kafkaParams));


    JavaDStream<String> mapPartDStream = kafkaDStream.mapPartitions(new RecordMapPartFunc());

    kafkaDStream.foreachRDD(rdd->{
      OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
      Map<Integer, Long> partitionToOffset = new LinkedHashMap<>();
      for (int i = 0; i < offsetRanges.length; i++) {
        partitionToOffset.put(offsetRanges[i].partition(), offsetRanges[i].untilOffset());
      }

      if(!partitionToOffset.isEmpty()){
        saveOffsets(partitionToOffset);
      }

    });


    jssc.start();

    try {
      jssc.awaitTermination();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }


  @Test
  public void testKafkaDStreamSimpleCollect(){

    JavaStreamingContext jssc = new JavaStreamingContext(getSparkConf(), Durations.milliseconds(1000* 10));
    final String topic = "testStringPerf";
//    final String topic = "testKafkaApi";
    final String bootstrapServers = "ldsver51:9092";
    final Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", bootstrapServers);
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("auto.offset.reset", "earliest");
    kafkaParams.put("enable.auto.commit", "false");
//    kafkaParams.put("max.poll.records", "100");
    kafkaParams.put("group.id", "java-consumer-04-kafkaRDD");


    JavaInputDStream<ConsumerRecord<String, String>> kafkaDStream;
    if (new File(offsetFilePath).exists()) {
      // sdc中 numberOfPartitions= Utils.getNumberOfPartitions(getProperties()),
      // 从 props中读取 partitionCount 对应的值为 numberOfPartitions;
      /**
       * SDC-Cluster如何设置 KafkaRDD的分区数量
       ClusterRunner.doStart() -> ClusterRunner.getClusterSourceInfo() -> clusterSource.getConfigsToShip()
       -> ClusterKafkaSource.getConfigsToShip(){
       new ImmutableMap.Builder<String, String>().put("partitionCount", String.valueOf(getParallelism())){// BaseKafakSource.getParallelism()
       //
       originParallelism = kafkaValidationUtil[KafkaValidationUtil09].getPartitionCount(conf.metadataBrokerList,conf.topic,new HashMap<String, Object>(conf.kafkaConsumerConfigs),3,1000);{
       KafkaValidationUtil09.getPartitionCount(String metadataBrokerList,String topic){
       KafkaConsumer<String, String> kafkaConsumer = createTopicMetadataClient(metadataBrokerList, kafkaClientConfigs);
       List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topic);
       partitionCount = partitionInfoList.size();
       }
       }
       }
       }
       *
       */

//      int numberOfPartitions = createConsumerAndPartitionForTopic(topic);
//      Map<TopicPartition, Long> fromOffsets = getOffsetForDStream(topic, numberOfPartitions);

      Map<TopicPartition, Long> fromOffsets = getPartitionsWithStartOffset(topic, 10);

      kafkaDStream =
              KafkaUtils.createDirectStream(
                      jssc,
                      LocationStrategies.PreferConsistent(),
                      ConsumerStrategies.<String,String>Assign(new ArrayList<TopicPartition>(fromOffsets.keySet()), kafkaParams, fromOffsets)
              );
    } else {
      kafkaDStream  = KafkaUtils.createDirectStream(
              jssc,
              LocationStrategies.PreferConsistent(),
              ConsumerStrategies.<String,String>Subscribe(Arrays.asList(topic), kafkaParams)
      );

    }

//    kafkaDStream = KafkaUtils.createDirectStream(jssc,LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(Arrays.asList(topic), kafkaParams));

    JavaDStream<String> mapPartDStream = kafkaDStream.mapPartitions(new RecordMapPartFunc());

    kafkaDStream.foreachRDD(rdd->{
      OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
      Map<Integer, Long> partitionToOffset = new LinkedHashMap<>();
      for (int i = 0; i < offsetRanges.length; i++) {
        partitionToOffset.put(offsetRanges[i].partition(), offsetRanges[i].untilOffset());
      }

      if(!partitionToOffset.isEmpty()){
        saveOffsets(partitionToOffset);
      }

    });


    jssc.start();

    try {
      jssc.awaitTermination();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }


  @Test
  public void testKafkaDStream(){
    JavaStreamingContext jssc = new JavaStreamingContext(getSparkConf(), Durations.milliseconds(1000* 10));
    final String topic = "testStringPerf";
//    final String topic = "testKafkaApi";
    final String bootstrapServers = "ldsver51:9092";
    final Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", bootstrapServers);
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("auto.offset.reset", "earliest");
    kafkaParams.put("enable.auto.commit", "false");
//    kafkaParams.put("max.poll.records", "100");
    kafkaParams.put("group.id", "java-consumer-04-kafkaRDD");


    JavaInputDStream<ConsumerRecord<String, String>> kafkaDStream;
    if (new File(offsetFilePath).exists()) {
      // sdc中 numberOfPartitions= Utils.getNumberOfPartitions(getProperties()),
      // 从 props中读取 partitionCount 对应的值为 numberOfPartitions;
      /**
       * SDC-Cluster如何设置 KafkaRDD的分区数量
          ClusterRunner.doStart() -> ClusterRunner.getClusterSourceInfo() -> clusterSource.getConfigsToShip()
            -> ClusterKafkaSource.getConfigsToShip(){
               new ImmutableMap.Builder<String, String>().put("partitionCount", String.valueOf(getParallelism())){// BaseKafakSource.getParallelism()
                  //
                  originParallelism = kafkaValidationUtil[KafkaValidationUtil09].getPartitionCount(conf.metadataBrokerList,conf.topic,new HashMap<String, Object>(conf.kafkaConsumerConfigs),3,1000);{
                    KafkaValidationUtil09.getPartitionCount(String metadataBrokerList,String topic){
                      KafkaConsumer<String, String> kafkaConsumer = createTopicMetadataClient(metadataBrokerList, kafkaClientConfigs);
                      List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topic);
                      partitionCount = partitionInfoList.size();
                    }
                  }
               }
            }
       *
       */

//      int numberOfPartitions = createConsumerAndPartitionForTopic(topic);
//      Map<TopicPartition, Long> fromOffsets = getOffsetForDStream(topic, numberOfPartitions);

      Map<TopicPartition, Long> fromOffsets = getPartitionsWithStartOffset(topic, 10);

      kafkaDStream =
              KafkaUtils.createDirectStream(
                      jssc,
                      LocationStrategies.PreferConsistent(),
                      ConsumerStrategies.<String,String>Assign(new ArrayList<TopicPartition>(fromOffsets.keySet()), kafkaParams, fromOffsets)
              );
    } else {
      kafkaDStream  = KafkaUtils.createDirectStream(
              jssc,
              LocationStrategies.PreferConsistent(),
              ConsumerStrategies.<String,String>Subscribe(Arrays.asList(topic), kafkaParams)
      );

    }

//    kafkaDStream = KafkaUtils.createDirectStream(jssc,LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(Arrays.asList(topic), kafkaParams));

    JavaDStream<String> mapPartDStream = kafkaDStream.mapPartitions(new RecordMapPartFunc());

    kafkaDStream.foreachRDD(rdd->{
      OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
      Map<Integer, Long> partitionToOffset = new LinkedHashMap<>();
      for (int i = 0; i < offsetRanges.length; i++) {
        partitionToOffset.put(offsetRanges[i].partition(), offsetRanges[i].untilOffset());
      }

      if(!partitionToOffset.isEmpty()){
        saveOffsets(partitionToOffset);
      }

    });


    jssc.start();

    try {
      jssc.awaitTermination();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  @Test
  public void testSDCKafkaStream_3_1(){
    Pair<JavaStreamingContext, JavaInputDStream<ConsumerRecord<String, String>>> pair = getKafkaDStream();


    MyDriver.foreach(pair.getValue().inputDStream());

    JavaStreamingContext jssc = pair.getKey();
    jssc.checkpoint("E:\\studyAndTest\\sparkTemp\\checkpoint");
    jssc.start();
    try {
      jssc.awaitTermination();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }


  }

  @Test
  public void testEnsoSDCKafkaStream_3_2(){
    Pair<JavaStreamingContext, JavaInputDStream<ConsumerRecord<String, String>>> pair = getKafkaDStream();
    JavaInputDStream<ConsumerRecord<String, String>> kafaDStream = pair.getValue();

    MyDriver.foreach(kafaDStream.inputDStream());

    kafaDStream.foreachRDD(new MyCommitOffset(kafaDStream));

    JavaStreamingContext jssc = pair.getKey();
//    jssc.checkpoint("E:\\studyAndTest\\sparkTemp\\checkpoint");
    jssc.start();
    try {
      jssc.awaitTermination();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }


  }


  private String serializeKafkaPartitionOffset(Map<Integer, Long> partitionsToOffset) throws IOException {
    return objectMapper.writeValueAsString(partitionsToOffset);
  }

  private void writeOffsetsToMainOffsetFile(Map<Integer, Long> partitionToOffsetMap) throws IOException {
//    try(OutputStream os = fs.create(checkPointMarkerFilePath, true)) {}

    try (OutputStream os = new FileOutputStream(offsetFilePath)) {
      objectMapper.writeValue(os, new ClusterSourceOffsetJson(serializeKafkaPartitionOffset(partitionToOffsetMap), "1"));
    }

//    lastOffsetStoredTime = System.currentTimeMillis();
  }






  class RecordMapPartFunc implements FlatMapFunction<Iterator<ConsumerRecord<String, String>>,String> {
    @Override
    public Iterator<String> call(Iterator<ConsumerRecord<String, String>> records) throws Exception {
      ArrayList<String> list = new ArrayList<>();
      if(records!=null ){
        ObjectMapper objectMapper = new ObjectMapper();
        HashMap<Integer,LinkedHashMap<Long,String>> partitions = new HashMap<>();

        // requestOffset < part.untilOffset
        AtomicLong counter = new AtomicLong(0);
        while (records.hasNext()){ // KafkaRDDIterator.hasNext(){ return requestOffset < part.untilOffset } 只要 offset 还没达到 传入的 untilOffset结束位置,就继续拉去
          ConsumerRecord<String, String> record = records.next();
          String key = record.key();
          long offset = record.offset();
          int partition = record.partition();
          LinkedHashMap<Long,String> offsets = partitions.getOrDefault(partition, new LinkedHashMap<>());
          try {
            JsonNode jsonNode = objectMapper.readTree(record.value());
            jsonNode.has("value");
            if(jsonNode.has("value")){
              double value1 = jsonNode.get("value").asDouble(0.0);
              offsets.put(offset,jsonNode.get("value").asText());
            }
          } catch (IOException e) {
            offsets.put(offset,record.value());
          }
          partitions.put(partition,offsets);

          long curr = counter.incrementAndGet();
          if(curr %30000==0){
            partitions.forEach((p,off)->{
              String msg = "Kafka分区-"+p+" : size="+off.size();
              System.err.println(msg);
            });
          }

        }

        partitions.forEach((k,v)->{
          long min = Long.MAX_VALUE;
          long max = Long.MIN_VALUE;
          for(Long offset:v.keySet()){
            if(offset < min) min = offset;
            if(offset > max) max = offset;
          }

          String msg = "Kafka分区-"+k+" : size="+v.size() + ", offsets["+min +" -> "+max+"]";
          list.add(msg);
        });

        if(partitions.isEmpty()){
          System.err.println("partitions.isEmpty(). 这批KafakRDD没数据");
        }

      }

      return list.iterator();
    }
  }

  class MyMapPartFunc implements FlatMapFunction<Iterator<String>,String> {
    @Override
    public Iterator<String> call(Iterator<String> stringIterator) throws Exception {

      return null;
    }
  }


  class MyMapFunc implements Function<ConsumerRecord<String, String>, String> {

    @Override
    public String call(ConsumerRecord<String, String> r) throws Exception {
      if(r!=null && r.value()!=null){
        int partition = r.partition();
        long offset = r.offset();

        return r.value();
      }else {
        return "NULL";
      }
    }
  }

  class MyCommitOffset implements VoidFunction<JavaRDD<ConsumerRecord<String, String>>> {
    private JavaInputDStream kafkaStream;

    public MyCommitOffset(JavaInputDStream<ConsumerRecord<String, String>> kafkaStream) {
      this.kafkaStream = kafkaStream;
    }

    @Override
    public void call(JavaRDD<ConsumerRecord<String, String>> consumerRecordJavaRDD) throws Exception {
      OffsetRange[] offsetRanges = ((HasOffsetRanges) consumerRecordJavaRDD.rdd()).offsetRanges();

      StringBuilder msg = new StringBuilder();
      for (OffsetRange o : offsetRanges) {
        if (o.fromOffset() < o.untilOffset()) {
          msg.append(o.topic() + "\t" + o.partition() + "\t" + o.fromOffset() + "\t" + o.untilOffset() + "\n");
        }
      }

      DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
      String time = df.format(new Date());

      System.out.println("[" + time + "]" + " committing offsets...");
      if (offsetRanges.length > 0) {
        ((CanCommitOffsets) kafkaStream.inputDStream()).commitAsync(offsetRanges);
      }

      time = df.format(new Date());
      System.out.println("[" + time + "]" + " offsets committed:\n" + msg);
    }
  }

  public static void main(String[] args) {
    new SparkJavaStudyDemo().testEnsoSDCKafkaStream_3_2();
  }

}
