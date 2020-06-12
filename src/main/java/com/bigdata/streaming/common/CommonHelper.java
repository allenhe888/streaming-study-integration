package com.bigdata.streaming.common;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AtomicDouble;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.sourceforge.argparse4j.impl.Arguments.store;


public class CommonHelper {
    private static CommonHelper instance = null;
    public synchronized static CommonHelper getInstance(){
        if(instance==null) instance = new CommonHelper();
        return instance;
    }


    public Namespace getNamespaceByArgsParser(String[] args, Map<String, String> params) {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser(this.getClass().getSimpleName())
                .defaultHelp(true)
                .description("This tool is used to verify the producer performance.");
        params.forEach((k,v)->addStringArgument(parser,k,false,v));
        try {
            Namespace namespace = parser.parseArgs(args);
            return namespace;
        } catch (ArgumentParserException e) {
            throw new RuntimeException(e);
        }
    }

    public Namespace getNamespaceByArgsParser(String[] args, Map<String, String> params,boolean addHelp,final String prefixChars) {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser(this.getClass().getSimpleName(),addHelp,prefixChars,null)
                .description("This tool is used to verify the producer performance.");
        params.forEach((name,defaultValue)->{
            parser.addArgument(prefixChars+name)
                    .action(store())
                    .required(false)
                    .type(String.class)
                    .metavar(name.toUpperCase())
                    .setDefault(defaultValue)
                    .dest(name)
                    .help(name +" is required: "+false);
        });
        try {
            Namespace namespace = parser.parseArgs(args);
            return namespace;
        } catch (ArgumentParserException e) {
            throw new RuntimeException(e);
        }
    }



    public Namespace getArgsNamespaceByKVMapWithoutSeparator(Map<String, String> params){
        List<String> list = new ArrayList<>();
        HashMap<String, String> buildMap = new HashMap<>();
        params.forEach((k,v)->{
            list.add("--"+k.toString());
            list.add(v.toString());
            buildMap.put(k,v.toString());
        });
        String[] arrays = list.toArray(new String[list.size()]);


        Namespace namespace = getNamespaceByArgsParser(arrays, buildMap);
        System.out.println(namespace);
        return namespace;
    }

    public void addStringArgument(ArgumentParser parser, String name,final boolean required,Object defaultValue) {
        parser.addArgument("--"+name)
                .action(store())
                .required(required)
                .type(String.class)
                .metavar(name.toUpperCase())
                .setDefault(defaultValue)
                .dest(name)
                .help(name +" is required: "+required);
    }

    public Path getFilePathIfNeedMakeDir(String dir, String fileName) {
        Path path = Paths.get(dir);
        if(Files.notExists(path)){
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        return path.resolve(fileName);
    }


    public static double divideRateAsSecond(final long size,final long millisTime,int scale){
        if(scale<0) scale=2;
        double result =0.0;
        try{
            BigDecimal divide = BigDecimal.valueOf(millisTime).divide(BigDecimal.valueOf(1000));
            result = BigDecimal.valueOf(size).divide(divide, scale, RoundingMode.HALF_UP).doubleValue();
        }catch (Exception e){
            e.printStackTrace();
            System.out.println(size+" / "+millisTime);
        }
        return result;
    }

    public static double divideRateAsSecond(final long size,final double millisTime,int scale){
        if(scale<0) scale=2;
        double result =0.0;
        try{
            BigDecimal divide = BigDecimal.valueOf(millisTime).divide(BigDecimal.valueOf(1000));
            result = BigDecimal.valueOf(size).divide(divide, scale, RoundingMode.HALF_UP).doubleValue();
        }catch (Exception e){
            e.printStackTrace();
            System.out.println(size+" / "+millisTime);
        }
        return result;
    }


    public DataGenerator dataGenerator = new DataGenerator();
    protected class DataGenerator{

        public JSONObject generateAnSingleRecord(String orgId, String modelId, String assetId, String pointId, Map<String, Object> rootDataKV){
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

        public JSONObject getJsonRecord(){
            return generateAnSingleRecord("orgId", "modelId", "assetId", "pointId", null);
        }

        public List<JSONObject> getJsonRecords(int recordNum) {
            ArrayList<JSONObject> list = new ArrayList<>();
            for (int i = 0; i < recordNum; i++) {
                list.add(getJsonRecord());
            }
            return list;
        }

        public List newFixBytesList(int mbSize,long sleepMillis){
            class TestData{
                private final byte[] bytes;
                public TestData() {
                    this.bytes = new byte[1024*1024];
                }
            }
            ArrayList<TestData> list = new ArrayList<>();
            for (int i = 0; i < mbSize; i++) {
                try {
                    if(sleepMillis >0) Thread.sleep(sleepMillis);
                } catch (InterruptedException e) {e.printStackTrace();}
                list.add(new TestData());
            }
            System.err.println(" 完成 List<TestData> 创建, List<TestData>.size= "+list.size() +" Mb");
            return list;
        }

    }

    public CollectHelper collectHelper = new CollectHelper();
    protected class CollectHelper{
        private int maxSize;
        public LinkedBlockingQueue<Double> createQueue(int size){
            this.maxSize = size;
            return new LinkedBlockingQueue<>();
        }

        public Double calculateQueueSum(Queue<java.lang.Double> queue){
            if(queue==null && queue.isEmpty()){
                return 0.0;
            }
            while(queue.size()>this.maxSize){
                queue.remove();
            }
            AtomicDouble sum = new AtomicDouble(0.0);
            queue.forEach(num->{
                sum.addAndGet(num);
            });
            return sum.get();
        }


    }



    public static JDBCHelper getJDBCHelper(int poolSize,String url,String username,String password){
        return new JDBCHelper(poolSize,url,username,password);
    }



    protected static class JDBCPool{
        private LinkedBlockingQueue<Connection> connectionQueue;
        public JDBCPool(int poolSize,String url,String username,String password){
            try{
                this.connectionQueue =  new LinkedBlockingQueue<Connection>(poolSize);
                for (int i = 0; i < poolSize; i++) {
                    Connection connection = DriverManager.getConnection(url, username, password);
                    connectionQueue.add(connection);
                }
            }catch (Exception e){
                throw new RuntimeException(e);
            }
        }

        public JDBCPool(){
            this.connectionQueue =  new LinkedBlockingQueue<Connection>(10);
        }

        public synchronized  Connection getConnection(long timeout) throws InterruptedException {
            return connectionQueue.poll(timeout, TimeUnit.MICROSECONDS);
        }

        public synchronized void returnConnection(Connection connection){
            connectionQueue.offer(connection);
        }

    }



    public static class JDBCHelper{



        private LinkedBlockingQueue<Connection> connectionQueue;
        private final int poolSize;
        private final JDBCPool jdbcPool;
        private final String url;
        private final String username;
        private final String password;
        public JDBCHelper(int poolSize,String url,String username,String password){
            this.poolSize= poolSize;
            this.jdbcPool = new JDBCPool(poolSize,url,username,password);
            this.url = url;
            this.username = username;
            this.password = password;
            try {
                Class.forName("com.mysql.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        public synchronized  Connection getConnection() throws InterruptedException {
            try{
                if(connectionQueue ==null){
                    connectionQueue = new LinkedBlockingQueue<Connection>(poolSize);
                    for (int i = 0; i < poolSize; i++) {
                        Connection connection = DriverManager.getConnection(this.url, this.username, this.password);
                        connectionQueue.add(connection);
                    }
                }
            }catch (SQLException e){
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            return connectionQueue.poll(2, TimeUnit.SECONDS);
        }

        public synchronized void returnConnection(Connection connection){
            connectionQueue.offer(connection);
        }




        public static class SqlTask{
            private PreparedStatement ps;
            private Connection connection;

            public SqlTask(PreparedStatement ps, Connection connection) {
                this.ps = ps;
                this.connection = connection;
            }
            public PreparedStatement getPs() {
                return ps;
            }
            public Connection getConnection() {
                return connection;
            }
        }

        public static class SqlExecutor{
            private JDBCPool jdbcPool;
            private int timeoutMillis;
            public SqlExecutor(JDBCPool jdbcPool){
                this.jdbcPool= jdbcPool;
                this.timeoutMillis = 1000;
            }

            LinkedList<SqlTask> taskQueue = new LinkedList<>();
            AtomicBoolean canExecuteSql = new AtomicBoolean(false);

            public Connection getConnection(){
                Connection connection = null;
                int loopCount= 0;
                try {
                    do{
                        connection = jdbcPool.getConnection(timeoutMillis);
                        if(connection==null){
                            try {
                                Thread.sleep(timeoutMillis);
                            } catch (InterruptedException e1) {}
                        }
                        loopCount++;
                    }while (connection==null && loopCount<100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return connection;
            }


            public void addSQLTask(PreparedStatement ps,Connection connection){
                try {
                    if(ps!=null&& !ps.isClosed()){
                        canExecuteSql.set(false);
                        taskQueue.offer(new SqlTask(ps, connection));
                        canExecuteSql.set(true);
//                        runAllTask();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            public void startSQLQueryLoop() {
                Runnable sqlQueryLoop = ()->{
                    while (true){
                        if(canExecuteSql.get()&& !taskQueue.isEmpty()){
                            SqlTask task = taskQueue.poll();
                            if(null !=task && task.getPs()!=null){
                                try {
                                    task.getPs().executeLargeUpdate();
                                    jdbcPool.returnConnection(task.getConnection());
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }
                            }
                        }else {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {}
                            continue;
                        }
                    }
                };
                Thread thread = new Thread(sqlQueryLoop);
                thread.setName("JDBC-execution-loop");
                thread.start();
            }

        }

        SqlExecutor sqlExecutor;
        public synchronized SqlExecutor startSqlExecutor(){
            if(sqlExecutor == null ){
                sqlExecutor = new SqlExecutor(jdbcPool);
                sqlExecutor.startSQLQueryLoop();
            }
            return sqlExecutor;
        }

    }


    public static boolean currentThread(String threadName){
        String name = Thread.currentThread().getName();
        if(name.matches(threadName)){
            return true;
        }
        return false;
    }



    public static class TestForComm{
        @Test
        public void test(){
            String output = "Executor task launch worker for task";
            String thread = "Executor task launch worker for task 6";
            if(thread.startsWith(output)){
                String[] split = thread.split(" ");
                String taskId = split[split.length - 1];
                if(Integer.parseInt(taskId) % 2==0){
                    System.out.println(thread);
                }

            }
            CommonHelper.currentThread("Executor task launch worker for task *");
        }
        @Test
        public void testEvictingQueueImpl(){
            EvictingQueueImpl<Integer> queue = new EvictingQueueImpl<>(5);
            for (int i = 0; i < 100; i++) {
                queue.add(i);
            }
            System.out.println(queue.size());
            queue.forEach((e)-> System.out.println(e));
            System.out.println(queue.trySum());
            System.out.println(queue.tryAverage());

        }

    }

}
