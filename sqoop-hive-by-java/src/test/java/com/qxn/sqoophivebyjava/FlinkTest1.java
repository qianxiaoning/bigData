package com.qxn.sqoophivebyjava;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class FlinkTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        System.setProperty("HADOOP_USER_NAME", "shonqian");
        String columns = "id INT ,a INT";
        String mysql_source_table = "emp";
        String flink_source_table = "emp4";
        String base_sql = "CREATE TABLE %s (%s) " +
                "WITH (" +
                "'connector.type' = 'jdbc'," +
                "'connector.url' = 'jdbc:mysql://192.168.88.2:3306/pra?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=Asia/Shanghai'," +
                "'connector.driver' = 'com.mysql.cj.jdbc.Driver'," +
                "'connector.table' = '%s'," +
                " 'connector.username' = 'root'," +
                " 'connector.password' = 'root'" +
                " )";
        String source_ddl = String.format(base_sql, flink_source_table, columns, mysql_source_table);
        tableEnv.executeSql(source_ddl);
        Table dataTable = tableEnv.sqlQuery("select * from " + flink_source_table);
        // hive catalog
        String name = "hive-test";
        //数据库
        String defaultDatabase = "test";
        // Hive的hive-site.xml文件所在路径
        String hiveConfDir = "FlinkSQL/src/main/resources";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog(name, hive);
        tableEnv.useCatalog(name);
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("test");

        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsert("mysql_hive", dataTable);
        statementSet.execute();
    }
}
