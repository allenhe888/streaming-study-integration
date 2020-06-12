package com.bigdata.streaming.clickhouse.demo;

import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class ClickHouseJdbcDemo {

    static {
        try {
            Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Connection getConnectionFromDriver(String url) throws SQLException {
        Connection connection = DriverManager.getConnection(url);
        return connection;
    }

    @Test
    public void testGetClickHouseDriver() {
        Connection conn = null;
        Statement statement = null;
        ResultSet resultSet= null;
        try {
            conn = getConnectionFromDriver("jdbc:clickhouse://ldsver51:9000/default");
            statement = conn.createStatement();

            resultSet = statement.executeQuery("select * from tb_memory_01");
            ResultSetMetaData metaData = resultSet.getMetaData();
            ArrayList<Object> rawResults = new ArrayList<>();
            while(resultSet.next()){
                HashMap<String, String> map = new HashMap<>();
                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    map.put(metaData.getColumnName(i),resultSet.getString(i));
                }
                rawResults.add(map);
            }

            rawResults.forEach(row->{
                System.out.println(row);
            });
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try{
                if(resultSet!=null){
                    resultSet.close();
                }
                if(statement!=null){
                    statement.close();
                }
                if(conn!=null){
                    conn.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }



    }

}
