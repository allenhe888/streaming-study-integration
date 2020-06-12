package com.bigdata.streaming.kafka.common;

import com.alibaba.fastjson.JSONObject;
import com.bigdata.streaming.common.CommonHelper;
import com.google.common.collect.ImmutableMap;
import javafx.util.Pair;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaHelper extends CommonHelper {


    public static void printByteBufferMessageSet(ByteBufferMessageSet messageAndOffsets) throws UnsupportedEncodingException {
        for(MessageAndOffset msg:messageAndOffsets){
            ByteBuffer payload = msg.message().payload();
            byte[] valueBytes = new byte[payload.limit()];
            payload.get(valueBytes);

            ByteBuffer key = msg.message().key();
            byte[] keyBytes = new byte[key.limit()];
            key.get(keyBytes);

            long offset = msg.offset();
            int size = msg.message().size();
            long timestamp = msg.message().timestamp();
            System.out.printf("\n Offset:%d , key=%s , payload=%s , size=%d , timestamp=%d \n",offset,
                    new String(keyBytes,"UTF-8"),
                    new String(valueBytes,"UTF-8"),
                    size,timestamp);
        }
    }

    public static String topic = "testKafkaApi";

    // consumer相关参数:
    public static String brokerList = "ldsver51:9092";
    public static String groupId = "ideaConsumerGroup";
    public static String clientId = "client-idea";
    public static String autoOffsetReset = "earliest";
    public static String enableAutoCommit = "false";

    public static String byteDeserializer = ByteArrayDeserializer.class.getName();
    public static String byteSerializer = ByteArraySerializer.class.getName();

    public static String bootstrapServers = "ldsver51:9092";
    public static String acls = "all";
    public static String stringDeserializer = StringDeserializer.class.getName();
    public static String stringSerializer = StringSerializer.class.getName();


    public  void printQPSPerBatch(long thisBatchSize,double time){
        double elapsedSec = time/1000.0;
        System.out.printf("BatchSize=%d (%.4f万),\t  用时elapsed=%.3f秒,\t  QPS=%.3f (%.3f万/sec) \n",
                thisBatchSize,thisBatchSize/10000.0,
                elapsedSec,
                thisBatchSize/elapsedSec, thisBatchSize/(10000*elapsedSec)
        );
    }

    public  JSONObject generateAnSingleRecord(String orgId, String modelId, String assetId, String pointId, Map<String, Object> rootDataKV){
        JSONObject record = new JSONObject();
        record.put("orgId", orgId);
        record.put("modelId", modelId);
        record.put("modelIdPath", "/".concat(modelId));
        record.put("assetId", assetId);
        record.put("pointId", pointId);
        record.put("time", System.currentTimeMillis());
        record.put("value", Math.random());
        record.put("attr", ImmutableMap.of("calQuality",0,"id",1));
        record	.put("dq", 0L);
        record	.put("quality", 0);
        if(rootDataKV!=null && !rootDataKV.isEmpty()){
            record.putAll(rootDataKV);
        }
        return record;
    }



    public  String[] parsedConsumerArags(String... args) {
        String brokerList= "ldsver51:9092";
        String topic= "testStringPerf";
        String group= "testGroup";
        String recordNum= "1000000";

        if(args.length>0){
            brokerList = args[0];
        }
        if(args.length>1){
            topic = args[1];
        }
        if(args.length>2){
            group = args[2];
        }
        if(args.length>3){
            recordNum = args[3];
        }

        String[] parsedArgs = {brokerList, topic, group, recordNum};
        return parsedArgs;
    }


    public ProducerHelp producerHelper = new ProducerHelp();

    public static class MyCallback implements Callback{
        private final long start;
        private final int iteration;
        private final int bytes;
        private final StateView stats;


        public MyCallback(int iter, long start, int bytes, StateView stats) {
            this.start = start;
            this.stats = stats;
            this.iteration = iter;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            this.stats.recordOnce(iteration, latency, bytes, now);
            if (exception != null)
                exception.printStackTrace();
        }

    }

    public static class StateView{
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
        private int iteration;
        private int windowIndex;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long reportingInterval;

        public StateView(long numRecords,long reportingInterval) {
            this.start= System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.iteration = 0;
            this.sampling = (int) (numRecords / Math.min(numRecords,10000*10) );
            this.latencies = new int[ (int)(numRecords / this.sampling) +1];
            this.windowIndex = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
        }

        public void recordOnce(int iter,int latency,int bytes , long time){
            this.count++;
            this.bytes+=bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency,latency);

            this.windowCount++;
            this.windowBytes +=bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(this.windowMaxLatency,latency);
            if( iter %sampling ==0){
                this.latencies[windowIndex]=latency;
                this.windowIndex++;
            }

            if(time - windowStart >= reportingInterval){
                printWindow();

                this.windowStart = System.currentTimeMillis();
                this.windowCount = 0;
                this.windowMaxLatency = 0;
                this.windowTotalLatency = 0;
                this.windowBytes = 0;
            }
        }


        public Callback nextCompletion(long start, int bytes, StateView stats){
            Callback cb = new MyCallback(this.iteration, start, bytes, stats);
            this.iteration++;
            return cb;
        }

        private void printWindow() {
            long elapse = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) elapse;

            System.out.printf("窗口:%d,\t 数量:%d,\t QPS:%.2f (%.4f万),\t\t 单Record均用时:%.2f毫秒 ,\t 窗口内MaxLatency:%.3f  \n",
                    windowIndex,
                    windowCount,
                    recsPerSec,
                    recsPerSec/10000,
                    windowTotalLatency / (double) windowCount,
                    (double) windowMaxLatency);
        }


        public void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;

            System.out.printf("总record数:%d,\t QPS:%.2f (%.4f万) 个/sec,\t 平均速率:%.2f mills/个,   MaxLatency:%.2f \n",
                    count,
                    recsPerSec,
                    recsPerSec/10000,
                    totalLatency / (double) count,
                    (double) maxLatency
            );
        }

    }


    protected class ProducerHelp{

        public <K,V> void testProducerRun(KafkaProducer<K, V> producer, String topic, int numRecords, List<Pair<K,V>> sampleData) {
            if(sampleData==null || sampleData.isEmpty()) throw new IllegalArgumentException("sampleData cannot be empty"+sampleData);
            StateView stateView = new StateView(numRecords, 3000);
            ProducerRecord<K,V> record;
            Random random = new Random();
            AtomicInteger flushCounter = new AtomicInteger(0);
            for(int i=0;i<numRecords;i++){
                Pair<K, V> kv = sampleData.get(random.nextInt(sampleData.size()));
                record = new ProducerRecord<K,V>(topic, kv.getKey(),kv.getValue());
                Callback callback = stateView.nextCompletion(System.currentTimeMillis(), kv.toString().getBytes().length, stateView);
                producer.send(record,callback);
//                if(flushCounter.incrementAndGet()>2000){
//                    producer.flush();
//                }
            }
            producer.flush();
            producer.close();
            stateView.printTotal();
        }

        public <K,V> void runProducerInFixRate(KafkaProducer<K, V> producer, String topic,final int maxBatches,final int sizePerBatch,final int sleepMillsPerBatch, List<Pair<K,V>> sampleData) {
            if(sampleData==null || sampleData.isEmpty()) throw new IllegalArgumentException("sampleData cannot be empty"+sampleData);
            ProducerRecord<K,V> record;
            Random random = new Random();
            boolean limitBatchs = true;
            if(maxBatches<=0){
                limitBatchs = false;
            }
            int batchCount = 0;
            while (limitBatchs && batchCount <=maxBatches){
                for(int i=0;i<sizePerBatch;i++){
                    Pair<K, V> kv = sampleData.get(random.nextInt(sampleData.size()));
                    record = new ProducerRecord<K,V>(topic, kv.getKey(),kv.getValue());
                    producer.send(record);
                }
                batchCount++;
                producer.flush();
                System.out.println(batchCount+"-batch 发送完, 发送了: "+sizePerBatch);
                try {
                    Thread.sleep(sleepMillsPerBatch);
                } catch (InterruptedException e) {}
            }
            producer.flush();
            producer.close();
        }



        public <K,V> KafkaProducer<K, V> getProducer(Map<String,Object> overrideProps){
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            props.put(ProducerConfig.ACKS_CONFIG,acls);
            props.put(ProducerConfig.RETRIES_CONFIG,3);

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSerializer);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, stringSerializer);

            if(overrideProps!=null && !overrideProps.isEmpty()){
                props.putAll(overrideProps);
            }

            return new KafkaProducer<>(props);
        }

        public <K,V> KafkaProducer<K, V> getTransactionProducer(Map<String,Object> overrideProps){
            String txProducerClientId = "clientId-tx-producer-common";
            String commTransactionId = "transactionId-common";
            Properties props = new Properties();
            props.put("bootstrap.servers",bootstrapServers);
            props.put("acks",acls);
            props.put(ProducerConfig.RETRIES_CONFIG,3);

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSerializer);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, stringSerializer);

            ImmutableMap<String, Object> transactionProps = ImmutableMap.<String, Object>builder()
                    .put(ProducerConfig.BATCH_SIZE_CONFIG, 100)
                    .put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432)
                    .put(ProducerConfig.LINGER_MS_CONFIG, 1000)// Linge:逗留时间,缓存同一批次最长逗留时间;
                    .put(ProducerConfig.RETRIES_CONFIG, 3)
                    .put(ProducerConfig.CLIENT_ID_CONFIG, txProducerClientId)
                    .put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, commTransactionId)
                    .put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)// Idempotence:幂等性;
                    .build();

            props.putAll(transactionProps);

            if(overrideProps!=null && !overrideProps.isEmpty()){
                props.putAll(overrideProps);
            }

            return new KafkaProducer<>(props);
        }

        protected  void sendToTopicWithNumRecords(String topic, int numRecords, String servers) {
            KafkaProducer<String, String> producer = getProducer(ImmutableMap.<String,Object>builder()
                    .put(ProducerConfig.BATCH_SIZE_CONFIG,100)
                    .put(ProducerConfig.RETRIES_CONFIG,20)
                    .put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true)
                    .build());
            try {
                for(int i=0;i<numRecords;i++){
                    producer.send(new ProducerRecord<>(topic,String.valueOf(i),String.valueOf(i)));
                }
                producer.flush();
            }finally {
                producer.close();
            }
        }

        protected <K,V>  KafkaProducer<K, V>  createTransactionalProducer(String transactionalId, String servers) {
            KafkaProducer<K, V> transactionProducer = getTransactionProducer(ImmutableMap.of(
                    ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId,
                    ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1,
                    ProducerConfig.RETRIES_CONFIG, 1000,
                    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true
            ));

            return transactionProducer;
        }

        protected ProducerRecord<String, String> generateStringProducerRecord(String topic, String key, String value,final boolean shouldCommit) {
            Header header = new Header() {
                @Override
                public String key() {
                    return "transactionStatus";
                }
                @Override
                public byte[] value() {
                    if (shouldCommit) {
                        return "committed".getBytes();
                    } else {
                        return "aborted".getBytes();
                    }
                }
            };
            return new ProducerRecord<String, String>(topic,null, key, value, Collections.singleton(header));
        }

        public  void shutdownExecutorServer(ExecutorService es) throws InterruptedException {
            es.shutdown();
            while (!es.awaitTermination(2000,TimeUnit.MILLISECONDS)){
                System.out.println("等待线程结束"+es);
                es.shutdownNow();
            }
            es = null;
            System.out.println("ExecutorService 关闭, 运行结束! ");
        }


    }



    @Test
    public void testArgsParse() throws ArgumentParserException {
        String[] args = {"--master","local[2]",
//                "--topics","testKafkaApi",
//                "--brokerServers","ldsver51:9092",
                "--consumerGroup","spark-streaming-test"
        };




        Namespace res = getNamespaceByArgsParser(args, ImmutableMap.of(
                "brokerServers", "ldsver51:9092",
                "topics", "testKafkaApi",
                "consumerGroup", "group-spark-streaming",
                "master", "local[3]"
        ));

        String master = res.getString("master");
        String brokerServers = res.getString("brokerServers");
        String topics = res.getString("topics");
        String consumerGroup = res.getString("consumerGroup");

        System.out.println(res);


    }



}
