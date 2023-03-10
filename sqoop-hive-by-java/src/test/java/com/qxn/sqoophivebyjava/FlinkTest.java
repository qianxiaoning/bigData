package com.qxn.sqoophivebyjava;

import java.sql.DriverManager;
import java.sql.SQLException;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class FlinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        System.setProperty("HADOOP_USER_NAME", "shonqian");
        DataStream<List<String>> streamSource = env.addSource(new MyMysqlSource("select * from emp"));
        streamSource.print();
        streamSource.addSink(new MyHiveSink("insert into emp4(id,a) values(?,?)"));
        env.execute();

    }

    /**
     * 自定义 Mysql Source Mysql建表
     * <p>
     * CREATE TABLE `mysql_hive`  (
     * `id` int(11) NULL DEFAULT NULL,
     * `name` varchar(255) ,
     * `age` int(11) NULL DEFAULT NULL,
     * `money` double NULL DEFAULT NULL,
     * `todate` date NULL DEFAULT NULL,
     * `ts` timestamp NULL DEFAULT NULL
     * ) ;
     */
    public static class MyMysqlSource extends RichSourceFunction<List<String>> {
        private String sql;
        private Connection conn = null;
        private PreparedStatement pstm = null;
        private ResultSet rs = null;

        public MyMysqlSource(String sql) {
            this.sql = sql;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            String driver = "com.mysql.cj.jdbc.Driver";
            String url = "jdbc:mysql://192.168.30.81:3306/pra?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=Asia/Shanghai";
            String username = "root";
            String password = "root";
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
            pstm = conn.prepareStatement(sql);
        }

        @Override
        public void run(SourceContext<List<String>> sourceContext) throws Exception {
            rs = pstm.executeQuery();
            int count = rs.getMetaData().getColumnCount();
            ArrayList<String> bean = new ArrayList<>();
            while (rs.next()) {
                bean.clear();
                for (int i = 1; i <= count; i++) {
                    bean.add(rs.getString(i));
                }
                sourceContext.collect(bean);
            }

        }

        @Override
        public void cancel() {

        }

        @Override
        public void close() throws Exception {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (pstm != null) {
                try {
                    pstm.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * 自定义 Hive Sink  Hive的建表语句
     * <p>
     * CREATE TABLE `mysql_hive`  (
     * `id` int,
     * `name` string ,
     * `age` int,
     * `money` double,
     * `todate` date,
     * `ts` timestamp
     * ) ;
     */
    public static class MyHiveSink extends RichSinkFunction<List<String>> {
        private PreparedStatement pstm;
        private Connection conn;
        private String sql;

        public MyHiveSink(String sql) {
            this.sql = sql;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = getConnection();
            pstm = conn.prepareStatement(sql);

        }

        @Override
        public void invoke(List<String> value, Context context) throws Exception {
            for (int i = 1; i <= value.size(); i++) {
                pstm.setString(i, value.get(i - 1));
            }
//            pstm.execute();
            pstm.executeUpdate();

        }

        @Override
        public void close() {
            if (pstm != null) {
                try {
                    pstm.close();
                } catch (SQLException e) {

                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        private static Connection getConnection() {
            Connection conn = null;
            try {
                String jdbc = "org.apache.hive.jdbc.HiveDriver";
                String url = "jdbc:hive2://192.168.30.117:10000/default";
                String user = "shonqian";
                String password = "";
                Class.forName(jdbc);
                conn = DriverManager.getConnection(url, user, password);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            return conn;
        }
    }
}

