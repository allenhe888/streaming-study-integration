package com.bigdata.streaming.kafka.consumer.exactlyonce;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @Description: 实现Kafka的精确一次消费
 * @author: HuangYn
 * @date: 2019/10/15 21:10
 */
public class ExactlyOnceConsumer {

    private final KafkaConsumer<String, String> consumer;
    private Map<TopicPartition, Long> tpOffsetMap;
    private List<ConsumerRecord> list;
    private String groupId;
    private String topic;

    public ExactlyOnceConsumer(Properties props, String topic, String groupId) {
        this.consumer = new KafkaConsumer<String, String>(props);
        this.list = new ArrayList<>(100);
        this.tpOffsetMap = new HashMap<>();
        this.groupId = groupId;
        this.topic = topic;
        this.consumer.subscribe(Arrays.asList(this.topic), new HandleRebalance());
    }

    public void receiveMsg() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                if (!records.isEmpty()) {
                    // 处理每个partition的记录
                    records.partitions().forEach(tp -> {
                        List<ConsumerRecord<String, String>> tpRecords = records.records(tp);
                        // 记录加到缓存中
                        tpRecords.forEach(record -> {
                            System.out.println("partition=" + record.partition() +", offset= " + record.offset() + ", value=" + record.value());
                            list.add(record);
                        });
                        // 将partition对应的offset加到map中, 获取partition中最后一个元素的offset，
                        // +1 就是下一次读取的位移，就是本次需要提交的位移
                        tpOffsetMap.put(tp, tpRecords.get(tpRecords.size() - 1).offset() + 1);
                    });
                }
                // 缓存中有数据
                if (!list.isEmpty()) {
                    // 将数据插入数据库，并且将位移信息也插入数据库
                    // 因此，每次读取到数据，都要更新本consumer在数据库中的位移信息
                    boolean success = insertIntoDB(list, tpOffsetMap);
                    if (success) {
                        list.clear();
                        tpOffsetMap.clear();
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    private boolean insertIntoDB(List<ConsumerRecord> list,
                                 Map<TopicPartition, Long> tpOffsetMap) {

        try {
            // TODO 将数据入库，这里省略了

            // 将partition位移更新
            String sql = "UPDATE kafka_info SET offsets = ? WHERE topic_group_partition = ?";
            List<Object[]> params = new ArrayList<>(tpOffsetMap.size());
            tpOffsetMap.forEach((tp, offset) -> {
                Object[] param = new Object[]{offset, topic + "_" + groupId + "_" + tp.partition()};
                params.add(param);
            });
//            jdbcHelper.batchExecute(sql, params);
            return true;
        } catch (Exception e) {
            // 回滚事务
        }
        return false;
    }

    /**
     * rebalance触发的处理器
     */
    private class HandleRebalance implements ConsumerRebalanceListener {
        // rebalance之前触发
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            //发生Rebalance时,只需要将list中数据和记录offset信息清空即可
            //这里为什么要清除数据,应为在Rebalance的时候有可能还有一批缓存数据在内存中没有进行入库，
            //并且offset信息也没有更新,如果不清除,那么下一次还会重新poll一次这些数据,将会导致数据重复
            System.out.println("==== onPartitionsRevoked ===== ");
            list.clear();
            tpOffsetMap.clear();
        }

        // rebalance后调用,consumer抓取数据之前触发
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.println("== onPartitionsAssigned ==");

            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            // 从数据库读取当前partition的信息
            Map<TopicPartition, Long> partitionOffsetMapFromDB = getPartitionOffsetMapFromDB(partitionInfos.size());

            // 在分配分区时指定消费位置
            for (TopicPartition partition : partitions) {
                // 指定consumer在每个partition上的消费开始位置
                // 如果在数据库中有对应partition的信息则使用，否则将默认从offset=0开始消费
                if (partitionOffsetMapFromDB.get(partition) != null) {
                    consumer.seek(partition, partitionOffsetMapFromDB.get(partition));
                } else {
                    consumer.seek(partition, 0L);
                }
            }
        }
    }

    /**
     * 从数据库读取offset信息
     *
     * @param size
     * @return
     */
    private Map<TopicPartition, Long> getPartitionOffsetMapFromDB(int size) {
        Map<TopicPartition, Long> partitionOffsetMapFromDB = new HashMap<>();
        String sql = "SELECT partition_num, offsets FROM kafka_info WHERE topic_group = ?";

        queryTPOffsetsFromDB(topic,groupId,partitionOffsetMapFromDB);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return partitionOffsetMapFromDB;
    }

    private void queryTPOffsetsFromDB(String topic, String groupId, Map<TopicPartition, Long> partitionOffsetMapFromDB) {

    }

}