package com.bigdata.streaming.common;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StaticCounter {
    private Map<String, AtomicLong> globalKeyCounter;
    private StaticCounter(){
        this.globalKeyCounter = new ConcurrentHashMap<>();
    }

    private static StaticCounter instance=null;
    public static StaticCounter getInstance(){
        if(instance==null){
            synchronized (StaticCounter.class){
                if(instance==null) instance = new StaticCounter();
            }
        }
        return instance;
    }


    private ThreadLocal<AtomicLong> threadLocalCounter= new ThreadLocal<>();
    public void createNewThreadCounter(){
        synchronized (this){
            if(threadLocalCounter==null){
                this.threadLocalCounter = new ThreadLocal<>();
            }else if(this.threadLocalCounter.get()!=null){
                this.threadLocalCounter.remove();
            }
            this.threadLocalCounter.set(new AtomicLong(0));
        }
    }
    public Long getThreadLocalValue(){
        if(threadLocalCounter!=null && this.threadLocalCounter.get()!=null){
            return this.threadLocalCounter.get().get();
        }
        return 0L;
    }
    public void removeThreadLocalCounter(){
        if(threadLocalCounter!=null){
            this.threadLocalCounter.remove();
        }
    }
    public void increaseTreadCounter(){
        synchronized (this){
            if(threadLocalCounter==null || this.threadLocalCounter.get()==null){
                createNewThreadCounter();
            }
        }
        this.threadLocalCounter.get().incrementAndGet();
    }


    private ThreadLocal<List<Long>> listThreadLocal= new ThreadLocal<>();
    public void createListThreadLocal(){
        synchronized (this){
            if(listThreadLocal==null){
                this.listThreadLocal = new ThreadLocal<>();
            }else if(this.listThreadLocal.get()!=null){
                this.listThreadLocal.get().clear();
                this.listThreadLocal.remove();
            }
            this.listThreadLocal.set(new LinkedList<Long>());
        }
    }
    public List<Long> getListThreadLocalValues(){
        if(listThreadLocal!=null && this.listThreadLocal.get()!=null){
            return this.listThreadLocal.get();
        }
        return Collections.emptyList();
    }
    public void removeListThreadLocal(){
        if(listThreadLocal!=null){
            if(listThreadLocal.get()!=null){
                listThreadLocal.get().clear();
            }
            this.listThreadLocal.remove();
        }
    }
    public Boolean addValueToListTreadLocal(Long ele){
        synchronized (this){
            if(listThreadLocal==null){
                createListThreadLocal();
            }else if(this.listThreadLocal.get()==null){
                createListThreadLocal();
            }
            List<Long> list = this.listThreadLocal.get();
            return list.add(ele);
        }
    }

    private ThreadLocal<AtomicInteger> threadBatchCounter= new ThreadLocal<>();
    private ThreadLocal<AtomicLong> threadTimeCounter= new ThreadLocal<>();
    public Long addThreadTimeAndBatchCount(Long ele){
        synchronized (this){
            // 计数+1;
            if(threadBatchCounter.get()==null){
                threadBatchCounter.set(new AtomicInteger(0));
            }
            threadBatchCounter.get().incrementAndGet();
            // 累加
            if(threadTimeCounter.get()==null){
                threadTimeCounter.set(new AtomicLong(0L));
            }
            return threadTimeCounter.get().addAndGet(ele);
        }
    }
    public Long getThreadTimeValue(){
        if(threadTimeCounter!=null && this.threadTimeCounter.get()!=null){
            return this.threadTimeCounter.get().get();
        }
        return 0L;
    }
    public Integer getThreadBatchValue(){
        if(threadBatchCounter!=null && this.threadBatchCounter.get()!=null){
            return this.threadBatchCounter.get().get();
        }
        return 0;
    }
    public void removeThreadTimeAndBatchCount(){
        synchronized (this){
            if(threadBatchCounter==null && threadBatchCounter.get()!=null){
                threadBatchCounter.remove();
            }
            if(threadTimeCounter==null && threadTimeCounter.get()!=null){
                threadTimeCounter.remove();
            }
        }
    }
    public void createThreadTimeAndBatchCount(){
        synchronized (this){
            if(threadBatchCounter==null){
                this.threadBatchCounter = new ThreadLocal<>();
            }
            this.threadBatchCounter.set(new AtomicInteger(0));
            if(threadTimeCounter==null){
                this.threadTimeCounter = new ThreadLocal<>();
            }
            this.threadTimeCounter.set(new AtomicLong(0));

        }
    }


    public synchronized long increment(String key){
        AtomicLong counter;
        synchronized (globalKeyCounter){
            counter = globalKeyCounter.get(key);
            if(counter==null){
                counter = new AtomicLong(0);
                globalKeyCounter.put(key,counter);
            }
            counter.incrementAndGet();
        }
        return counter.incrementAndGet();
    }

    public void incrementAndPrintCurrentValue(String key){
        System.err.println(key+" -> "+ increment(key));
    }

    public long get(String key){
        synchronized (globalKeyCounter){
            AtomicLong counter = globalKeyCounter.get(key);
            if(counter!=null){
                return counter.get();
            }
        }
        return 0L;
    }


    public static class Cache<T>{
        List<Double> list= new LinkedList<Double>();
        public void add(Double element){
            synchronized (list){
                list.add(element);
            }
        }

        public List getAll(){
            return list;
        }
        public Double sum(){
            double sum = 0.0;
            for(Double num:list){
                sum+= num;
            }
            return sum;
        }

        public Integer size(){
            return list.size();
        }

        public void clean(){
            list.clear();
        }

    }



    private ConcurrentHashMap<Integer,Cache> objectCaches= new ConcurrentHashMap<Integer,Cache>();
    public static <K, V> Cache<V> getCache(K obj) {
        int hash = obj.hashCode();
        if(getInstance().objectCaches.containsKey(hash)){
            Cache<V> cache = (Cache<V>) getInstance().objectCaches.get(hash);
            return cache;
        }
        Cache<V> cache = new Cache<V>();
        getInstance().objectCaches.put(hash,cache);
        return cache;
    }

    public static void cleanCache(){
        ConcurrentHashMap<Integer, Cache> objectCaches = getInstance().objectCaches;
        synchronized (objectCaches){
            objectCaches.forEach((k,v)->{
                v.list.clear();
            });
        }
    }


}


