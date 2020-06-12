//package com.bigdata.streaming.spark.demo;
//
//import org.junit.Test;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//
//public class TestJavaVM {
//
//    private static final int batchSize= 40;
//    private Map<String, List<Long>> map;
//    long[][] doubleArray;
//
//    public TestJavaVM() {
//        this.map= new ConcurrentHashMap<>();
//        this.doubleArray = new long[1000][batchSize];
//    }
//
//    public static void main(String[] args) throws InterruptedException {
//        TestJavaVM test = new TestJavaVM();
//        test.testJavaOOM();
//    }
//
//    @Test
//    public void testJavaOOM() throws InterruptedException {
//        int batch =0;
//        while (true){
//            batch ++;
//            Thread.sleep(10);
//            long current = System.currentTimeMillis();
//            List<Long> values = new ArrayList<>();
//            long[] tempArray = new long[batchSize];
//            for(int i=0;i<batchSize;i++){
//                long value = current + 1000*i;
//                values.add(value);
//                tempArray[i]= value;
//            }
//            map.put("batch_"+batch,values);
//            if(doubleArray.length <=batch){
//                doubleArray = Arrays.copyOf(doubleArray, doubleArray.length* 2);
//            }
//            doubleArray[batch] = tempArray;
//            System.out.println("等待完成, 当前 map.size="+map.size());
//        }
//    }
//
//}
