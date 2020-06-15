package com.bigdata.streaming.kafka.producer.enos;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.streaming.common.CommKey;
import com.bigdata.streaming.kafka.common.KafkaHelper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import javafx.util.Pair;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ProducerEnosRecord extends KafkaHelper {
    public int numRecords = 10000 * 100;


    /**
     *  样本文件中, 既有values,也定义了 keys,用于分区;
     * @param namespace
     */

    public void sendWithKeys(Namespace namespace) {
//        Namespace namespace = getNamespaceByArgsParser(args, ImmutableMap.<String, String>builder()
//                .put(CommKey.bootstrapServers, bootstrapServers)
//                .put(CommKey.topic, topic)
//                .put(CommKey.numRecords, "1000000")
//                .put(CommKey.sampleFile, "E:\\myWork\\Envision\\模型-设备\\GenerateSample-Assets-ModelPoints.txt")
//                .build());
        List<Pair<String,String>> keyWithValues = parseInputKeyValueFromJson(namespace.getString(CommKey.sampleFile));
        KafkaProducer<String, String> producer = producerHelper.getProducer(ImmutableMap.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, namespace.getString(CommKey.bootstrapServers),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSerializer,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, stringSerializer
        ));
        producerHelper.testProducerRun(producer,namespace.getString(CommKey.topic),Integer.parseInt(namespace.getString(CommKey.numRecords)), keyWithValues);

    }
    @Test
    public void testSendWithKeys(){
        String argsStr = "--bootstrapServers ldsver51:9092 --topic testStringPerf --numRecords 50000000 --canParsedKeys true --sampleFile E:\\myWork\\Envision\\模型-设备\\GenerateSample-Assets-ModelPoints.txt";
        doSendDataByDiffKeyPolicy(argsStr.split(CommKey.EMPTY_STRING));
    }

    /**
     * 发送 只有Record数据的 方式实现;
     * 默认key==nul时, 将把数据均匀分散在各分区;
     * @param namespace
     */

    public void sendFromSampleFile(Namespace namespace) {
        List<Pair<String, String>> keyWithValues = new ArrayList<>();
        try {
            for(String line:ioHelper.getLinesFromLocalFile(namespace.get(CommKey.sampleFile))){
                try{
                    JSON.parseObject(line);
                    keyWithValues.add(new Pair<>(null, line));
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        KafkaProducer<String, String> producer = producerHelper.getProducer(ImmutableMap.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, namespace.getString(CommKey.bootstrapServers),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSerializer,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, stringSerializer
        ));
        producerHelper.testProducerRun(producer,namespace.getString(CommKey.topic),Integer.parseInt(namespace.getString(CommKey.numRecords)), keyWithValues);

    }
    @Test
    public void testSendFromSampleFile(){
        String argsStr = "--bootstrapServers ldsver51:9092 --topic testTopic --numRecords 100 --canParsedKeys false --sampleFile E:\\studyAndTest\\inputs\\sdc-record-sample.json";
        doSendDataByDiffKeyPolicy(argsStr.split(CommKey.EMPTY_STRING));
    }
    @Test
    public void testHuangWenTestData(){
        String argsStr = "--bootstrapServers ldsver51:9092 --topic testTopic --numRecords 2000000 --canParsedKeys false --sampleFile E:\\studyAndTest\\inputs\\sdc-record-sample.json";
        doSendDataByDiffKeyPolicy(argsStr.split(CommKey.EMPTY_STRING));
    }




    /**
     * 共同 公共方法
     */
    public Namespace parseArgsAsNamespace(String[] args){
        Namespace namespace = getNamespaceByArgsParser(args, ImmutableMap.<String, String>builder()
                .put(CommKey.bootstrapServers, bootstrapServers)
                .put(CommKey.topic, topic)
                .put(CommKey.numRecords, "1000000")
                .put(CommKey.canParsedKeys, "false")
                .put(CommKey.sampleFile, "E:\\myWork\\Envision\\模型-设备\\GenerateSample-Assets-ModelPoints.txt")
                .build());
        return namespace;
    }

    public void doSendDataByDiffKeyPolicy(String[] args) {
        Namespace namespace = parseArgsAsNamespace(args);
        if(Boolean.parseBoolean(namespace.get(CommKey.canParsedKeys))){
            sendWithKeys(namespace);
        }else{
            sendFromSampleFile(namespace);
        }
    }

    private List<Pair<String,String>> parseInputKeyValueFromJson(String filePath) {
        File file = new File(filePath);
        if(!file.exists()){
            throw new IllegalArgumentException("not exist FilePath: "+filePath);
        }

//        Map<String, List<String>> keyWithRecordStr = new HashMap<>();
        List<Pair<String,String>> keyValueList = new ArrayList<>();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(file);
            ArrayList<String> list = new ArrayList<>();

            String orgId = "myOrgId";
            if(jsonNode.has("orgId")){
                String ou = jsonNode.get("orgId").asText();
                if(ou!=null && !ou.isEmpty()){
                    orgId = ou;
                }
            }
            if(jsonNode.has("assets")){
                JsonNode assets = jsonNode.get("assets");
                assets.size();
                for(JsonNode asset:assets){
                    String assetStr = asset.asText();
                    if(assetStr!=null && !assetStr.isEmpty()){
                        list.add(assetStr);
                    }
                }
            }

            if(list.isEmpty()){
                list.addAll(ImmutableList.of("testAsset_01","testAsset_02","testAsset_03"));
            }

            HashMap<String, List<String>> modelPoints = new HashMap();
            if(jsonNode.has("modelAndPoints")){
                JsonNode modelAndPoints = jsonNode.get("modelAndPoints");
                for(JsonNode node:modelAndPoints){
                    String modelPoint = node.get("inputPoint").asText();
                    if(modelPoint!=null && !modelPoint.isEmpty()){
                        String[] split = modelPoint.split("::");
                        if(split.length==2){
                            List<String> oneModelPoints = modelPoints.getOrDefault(split[0], new ArrayList<>());
                            oneModelPoints.add(split[1]);
                            modelPoints.put(split[0],oneModelPoints);
                        }
                    }
                }
            }

            if(!modelPoints.isEmpty()){
                modelPoints.forEach((k,v)-> System.out.println(v));
                final String ou = orgId;
                modelPoints.forEach((modelId,points)->{
                    for(String pointId:points){
                        for(String assetId:list){
                            JSONObject jsonRecord = dataGenerator.generateAnSingleRecord(ou,modelId,assetId,pointId,null);
                            keyValueList.add(new Pair(assetId,jsonRecord.toJSONString()));
                        }
                    }
                });
            }


        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("not exist FilePath: "+filePath);
        }
        return keyValueList;
    }





    @Test
    public void testSendDataInFixRate() {
        List<Pair<String,String>> keyWithValues = parseInputKeyValueFromJson("E:\\myWork\\Envision\\模型-设备\\GenerateSample-Assets-ModelPoints.txt");
        KafkaProducer<String, String> producer = producerHelper.getProducer(ImmutableMap.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ldsver51:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSerializer,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, stringSerializer
        ));
        producerHelper.runProducerInFixRate(producer,"HJQ_testKafkaPerPartition",1000,10,4000, keyWithValues);
    }



    public static void main(String[] args) {
        /*
        --bootstrapServers ldsver53:9092,ldsver53:9093,ldsver53:9094 --topic HJQ_testKafkaPerPartition --numRecords 100000 --sampleFile E:\myWork\Envision\模型-设备\GenerateSample-Assets-ModelPoints.txt
         */
        new ProducerEnosRecord().doSendDataByDiffKeyPolicy(args);

    }





}
