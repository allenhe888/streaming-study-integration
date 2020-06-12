package com.bigdata.streaming.common;

public class CommKey {
    public final static String EMPTY_STRING = " ";

    public static String numRecords = "numRecords";
    public static String brokerList = "brokerList";
    public static String sampleFile = "sampleFile";


    // Kafka Key
    public static String bootstrapServers = "bootstrapServers";
    public static String topic = "topic";
    public static String partitions = "partitions";
    public static String maxRatePerPartition = "maxRatePerPartition";
    public static String kafkaPollMaxRetries = "kafkaPollMaxRetries";
    public static String enableAutoCommit = "enableAutoCommit";
    public static String groupId = "groupId";
    public static String fetchMaxBytes = "fetchMaxBytes";
    public static String maxPollRecords = "maxPollRecords";
    public static String consumerPollMs = "consumerPollMs";
    public static String KeyDeserializer = "KeyDeserializer";
    public static String ValueDeserializer = "ValueDeserializer";
    public static String commitOffsetsForeach = "commitOffsetsForeach";
    public static String isPrintSucceed = "isPrintSucceed";

    public static String startOffset = "startOffset";
    public static String recordNum = "recordNum";
    public static String batchIntervalSec = "batchIntervalSec";
    public static String reportIntervalSec = "reportIntervalSec";
    public static String reFromOffset = "reFromOffset";
    public static String maxPartitionOffset = "maxPartitionOffset";
    public static String enableBackPressure = "enableBackPressure";

    // Spark Key
    public static String master = "master";
    public static String isRemote = "isRemote";
    public static String isClusterMode = "isClusterMode";
    public static String jars = "jars";
    public static String deployMode = "deployMode";
    public static String batchDuration = "batchDuration";
    public static String avgStartBatch = "avgStartBatch";
    public static String qpsQueueSize = "qpsQueueSize";
    public static String executorNum = "executorNum";
    public static String executorMemory = "executorMemory";


}
