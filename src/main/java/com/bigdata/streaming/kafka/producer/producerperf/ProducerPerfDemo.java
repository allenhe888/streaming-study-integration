package com.bigdata.streaming.kafka.producer.producerperf;

public class ProducerPerfDemo {

    static String bootstrapServers = "bootstrap.servers=ldsver51:9092";
    static String acls = "acls=all";
    static String topic = "testPerf";
    static String throughput = "-1";
    static String numRecords = String.valueOf(10000 * 100);
    static String recordSize = String.valueOf(250);

    public static void doMain(String brokerList,String topic,int numRecords,int recordSize) throws Exception {
        String[] args = new String[]{
                "--producer-props", "bootstrap.servers="+brokerList,acls,
                "--topic",topic,
                "--throughput",throughput,
                "--num-records",String.valueOf(numRecords),
                "--record-size",String.valueOf(recordSize)
        };
        ProducerPerformance.main(args);
    }

    public static void main(String[] args) throws Exception {
        if(args==null || args.length <1){
            args = new String[]{
                    "--producer-props", bootstrapServers,acls,
                    "--topic",topic,
                    "--throughput",throughput,
                    "--num-records",numRecords,
                    "--record-size",recordSize
            };
        }

        ProducerPerformance.main(args);
    }
}
