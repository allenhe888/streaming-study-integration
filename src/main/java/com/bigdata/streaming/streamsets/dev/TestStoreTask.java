package com.bigdata.streaming.streamsets.dev;

import com.alibaba.fastjson.JSONObject;
import com.bigdata.streaming.common.CommonHelper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TestStoreTask extends CommonHelper {

    @Test
    public void testWriteBigJson(){
        try{

            Path filePath = getFilePathIfNeedMakeDir("E:\\studyAndTest\\idea","uiinfo.json");

            ObjectMapper mapper = new ObjectMapper();
            for (int i = 0; i < 20000; i++) {
                List<JSONObject> records = dataGenerator.getJsonRecords(100000);
                HashMap<String, Object> uiInfo = new HashMap<>();
                uiInfo.put("records",records);
                uiInfo.put("size",records.size());
                uiInfo.put("time",System.currentTimeMillis());

                long start = System.currentTimeMillis();
                OutputStream outputStream = Files.newOutputStream(filePath);
                mapper.writeValue(outputStream,uiInfo);
                System.out.println("write()写入磁盘: usingTime:"+(System.currentTimeMillis() - start));

//                Thread.sleep(1000);
                InputStream inputStream = Files.newInputStream(filePath);
                start = System.currentTimeMillis();
                try{
                    mapper.readValue(inputStream, Map.class);
                }catch (Exception e){
                    e.printStackTrace();
                    throw e;
                }
                System.out.println("read() 读取完整Json数据 usingTime:"+(System.currentTimeMillis() - start));

                Thread.sleep(20);
            }


        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }


    @Test
    public void testWriteBigJsonThreadNoSafe(){
        try{
            Path filePath = getFilePathIfNeedMakeDir("E:\\studyAndTest\\idea","uiinfo.json");
            ExecutorService es = Executors.newFixedThreadPool(6);
            int loopNum = 10000;
            es.execute(()->{
                while (true){
                    try{
                        saveUI(filePath);
                    }catch (Exception e){e.printStackTrace();}
                }
            });
            es.execute(()->{
                while (true){
                    try{
                        getUiInfo(filePath);
                    }catch (Exception e){e.printStackTrace();}
                }
            });
            es.execute(()->{
                while (true){
                    try{
                        getUiInfo(filePath);
                    }catch (Exception e){e.printStackTrace();}
                }
            });
            es.execute(()->{
                while (true){
                    try{
                        getUiInfo(filePath);
                    }catch (Exception e){e.printStackTrace();}
                }
            });
            es.execute(()->{
                while (true){
                    try{
                        getUiInfo(filePath);
                    }catch (Exception e){e.printStackTrace();}
                }
            });
            es.execute(()->{
                while (true){
                    try{
                        saveUI(filePath);
                        getUiInfo(filePath);
                    }catch (Exception e){e.printStackTrace();}
                }
            });
            es.execute(()->{
                while (true){
                    try{
                        getUiInfo(filePath);
                        saveUI(filePath);
                    }catch (Exception e){e.printStackTrace();}
                }
            });
//            for (int i = 0; i < 20000; i++) {
//                es.execute(()->{
//                    for (int i = 0; i < loopNum; i++) {
//                        try{
//                            saveUI(filePath);
//                            getUiInfo(filePath);
//                        }catch (Exception e){
//                            e.printStackTrace();
////                            throw new RuntimeException(e);
//                        }
//                    }
//                });
//
//                Thread.sleep(1000);
//                es.execute(()->{
//                    for (int i = 0; i < loopNum; i++) {
//                        try{
//                            saveUI(filePath);
//                            getUiInfo(filePath);
//                        }catch (Exception e){
//                            e.printStackTrace();
////                            throw new RuntimeException(e);
//                        }
//                    }
//
//                });
                es.shutdown();
                while (!es.awaitTermination(5, TimeUnit.SECONDS)){
                    System.out.println("等待线程完成");
                }
                System.out.println(" 运行完: es.isShutdown="+es.isShutdown());
//            }
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    ObjectMapper mapper = new ObjectMapper();
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private synchronized void saveUI(Path filePath) throws IOException {
        List<JSONObject> records = dataGenerator.getJsonRecords(100);
        HashMap<String, Object> uiInfo = new HashMap<>();
        uiInfo.put("records",records);
        uiInfo.put("size",records.size());
        uiInfo.put("time",System.currentTimeMillis());
        saveUiInfo(filePath,uiInfo);

    }

    private synchronized void saveUiInfo(Path filePath, HashMap<String, Object> uiInfo) throws IOException {
//        saveUIByWriteLock(filePath,uiInfo);
        saveUiByWriteLockComputeWithReadMethod(filePath,uiInfo);
    }

    private synchronized void saveUIByWriteLock(Path filePath, HashMap<String, Object> uiInfo) throws IOException {
        readWriteLock.writeLock().lock();
        try{
            OutputStream outputStream = Files.newOutputStream(filePath);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            mapper.writeValue(outputStream,uiInfo);

        }finally {
            readWriteLock.writeLock().unlock();
        }
    }

    // 写入时, 不能读取; 同样,读取时,不能写入; 即读和写操作要用互斥;
    // 当没有写时, 需要多线线程能同时拿到 读的锁,能并发读;
    // 并发读时,需要并发读的锁;
    private synchronized void saveUiByWriteLockComputeWithReadMethod(Path filePath, HashMap<String, Object> uiInfo) throws IOException {
        readWriteLock.writeLock().lock();
        try{
            OutputStream outputStream = Files.newOutputStream(filePath);
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {e.printStackTrace();}
            new ObjectMapper().writeValue(outputStream,uiInfo);
        }finally {
            readWriteLock.writeLock().unlock();
        }
    }






    private void getUiInfo(Path filePath) throws IOException {
        getUiByReadLock(filePath);
    }
    private void getUiByReadLock(Path filePath) throws IOException {
        readWriteLock.readLock().lock();
        try{
            InputStream inputStream = Files.newInputStream(filePath);
//            try {
////                Thread.sleep(1000);
//            } catch (InterruptedException e) {e.printStackTrace();}
            new ObjectMapper().readValue(inputStream, Map.class);
        }finally {
            readWriteLock.readLock().unlock();
        }

    }


}
