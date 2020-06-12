package com.bigdata.streaming.kafka.consumer.commhelper;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class RebalanceListenerImpl implements ConsumerRebalanceListener {
    final private AtomicLong rebalanceCounter;
    final private AtomicBoolean isAssigned;

    public RebalanceListenerImpl(AtomicLong rebalanceCounter, AtomicBoolean isAssigned) {
        this.rebalanceCounter = rebalanceCounter;
        this.isAssigned = isAssigned;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("发送Rebalance了, 这是第n次Rebalance: "+rebalanceCounter.incrementAndGet());
        isAssigned.set(false);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("成功Rebalance! Assigned Partitons: ");
        Map<String,  List<Integer>> topics = new HashMap<>();
        partitions.forEach(tp->{
            List<Integer> sameTopic = topics.getOrDefault(tp.topic(),new ArrayList<Integer>());
            sameTopic.add(tp.partition());
            topics.put(tp.topic(),sameTopic);
        });
        topics.forEach((k,list)->{
            System.out.println("topic( "+k+" ): "+list.toString());
        });
        isAssigned.set(true);
    }
}
