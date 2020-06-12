package com.bigdata.streaming.common;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class EvictingQueueImpl<T> {
    private final int size;
    private final LinkedBlockingQueue<T> queue ;
    public EvictingQueueImpl(int size){
        this.size = size;
        this.queue = new LinkedBlockingQueue<T>();
    }

    public void add(T e){
        synchronized (this.queue){
            while(queue.size() > size){
                queue.poll();
            }
            queue.offer(e);
        }
    }
    public int size(){
        return queue.size();
    }
    public void forEach(Consumer<? super T> action){
        queue.forEach(action);
    }

    public double trySum(){
        double sum = 0.0;
        for(T e:queue){
            sum += new BigDecimal(e.toString()).doubleValue();
        }
        return sum;
    }
    public double tryAverage(){
        if(queue.size()>0){
            return BigDecimal.valueOf(trySum()).divide(BigDecimal.valueOf(queue.size()),3,RoundingMode.HALF_UP).doubleValue();
        }else {
            return 0.0;
        }
    }



}
