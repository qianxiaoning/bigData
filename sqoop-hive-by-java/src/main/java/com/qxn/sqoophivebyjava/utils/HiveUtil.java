package com.qxn.sqoophivebyjava.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.util.OptionsFileUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <简述> hive工具类
 * <详细描述> hiveUtil
 */
public class HiveUtil {
    // hadoop参数
    private static String hdfsUrl = PropertiesUtil.prop("hadoop.hdfsUrl");
    private static String hadoopUser = PropertiesUtil.prop("hadoop.hadoopUser");
    // hive参数
    private static String hiveDriverName = PropertiesUtil.prop("hive.hiveDriverName");
    private static String hiveUrl = PropertiesUtil.prop("hive.hiveUrl");
    private static String hiveDatabase = PropertiesUtil.prop("hive.hiveDatabase");
    private static String hiveUser = PropertiesUtil.prop("hive.hiveUser");
    private static String hivePassword = PropertiesUtil.prop("hive.hivePassword");

    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;

    public static void hiveInit() {
        // 创建hive驱动，一个hive请求占用一个Connection，多余请求是阻塞操作。
        // 如果hive请求并发量高，搭建hive连接池（百度）
        try {
            Class.forName(hiveDriverName);
            conn = DriverManager.getConnection(hiveUrl + hiveDatabase, hiveUser, hivePassword);
            stmt = conn.createStatement();
            // windows环境给与hadoop权限
            System.setProperty("HADOOP_USER_NAME", hadoopUser);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    /**
     * <简述> 删除hive表
     * <详细描述>
     * @param tableName hive表名
     * @return void
     */
    public static void dropTable(String tableName) {
        try {
            hiveInit();
            String sql = "drop table if exists " + tableName;
            stmt.execute(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            hiveDestory();
        }
    }

    // 查看表结构
    public static List<Map<String, String>> descTable(String hiveTableName) {
        List<Map<String, String>> fieldList = new ArrayList<>();
        try {
            hiveInit();
            String sql = "desc " + hiveTableName;
//        System.out.println("Running: " + sql);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                Map<String, String> fieldMap = new HashMap<>();
                fieldMap.put("name", rs.getString(1));
                fieldMap.put("type", rs.getString(2));
                fieldList.add(fieldMap);
            }
//            System.out.println(fieldList);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            hiveDestory();
        }
        return fieldList;
    }

    // 查询表是否存在
    public static Boolean seeTableIfExist(String hiveTableName) {
        try {
            hiveInit();
            String sql = "show tables like '" + hiveTableName + "'";
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
//            System.out.println(rs.getString(1));
                return true;
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            hiveDestory();
        }
        return false;
    }

    // 改变表结构
    public static void alterTable(String sql) {
        try {
            hiveInit();
//            System.out.println("Running: " + sql);
            stmt.execute(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            hiveDestory();
        }
    }

    // mysqlStructure2hive
    public static void mysqlStructure2hive(String databaseUrl, String account, String password,
                                           String tablename, String hiveTablename) {
        try {
            String[] args = new String[]{
                    "create-hive-table",
                    "--connect", "jdbc:mysql://" + databaseUrl + "?serverTimezone=Asia/Shanghai",
//                    "--connect", "jdbc:mysql://192.168.xx.xxx:3306/pra?serverTimezone=Asia/Shanghai",
                    "-username", account,
                    "-password", password,
                    "--table", tablename,
                    "--hive-table", hiveDatabase + '.' + hiveTablename,
                    "--fields-terminated-by","\t",
            };
            String[] expandArguments = OptionsFileUtil.expandArguments(args);
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", hdfsUrl);
            Sqoop.runTool(expandArguments, conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 复制hive表
    public static void copyHiveTable(String hiveTablename, String newHiveTablename) {
        try {
            hiveInit();
            String sql =
                    "create table if not exists " + newHiveTablename + " as select * from " + hiveTablename;
//            System.out.println("Running: " + sql);
            stmt.execute(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            hiveDestory();
        }
    }

    // 查找已有hive表指定字段到新表
    public static void copyMappingFields2Hive(String hiveTablenames, String newHiveTablename, String fields) {
        try {
            hiveInit();
            // 效果idMappings = "a a,b b";
            String sql =
                    "create table if not exists " + newHiveTablename + " as select " + fields + " from " + hiveTablenames;
//            System.out.println("Running: " + sql);
            stmt.execute(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            hiveDestory();
        }
    }

    // hive表插入数据
    public static void insertData(String tableName, String originalSql) {
        try {
            hiveInit();
            String sql = "insert into table " + tableName + " " + originalSql;
            stmt.execute(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            hiveDestory();
        }
    }

    // 创建hive表
    public static void createHiveTable() {
        try {
            hiveInit();
            String sql = "create table emp6(\n" +
                    "id int,\n" +
                    "a int\n" +
                    ")\n" +
                    "row format delimited fields terminated by '\\t'";
            System.out.println("Running: " + sql);
            stmt.execute(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            hiveDestory();
        }
    }

    // mysql2hdfs
    public static void mysql2hdfs() {
        try {
            hiveInit();
            String targetDir = "/test/5";
            String[] args = new String[]{
                    "import",
                    "--connect", "jdbc:mysql://192.168.xx.xxx:3306/pra?serverTimezone=Asia/Shanghai",
                    "-username", "root",
                    "-password", "root",
                    "--table", "emp",
                    "--null-string", "na",
                    "--null-non-string", "na",
                    "-m", String.valueOf(1),
                    "--outdir", "~/mySqoopTemp/",
                    "--delete-target-dir",
                    "--target-dir", targetDir,
                    "--fields-terminated-by","\t",
                    "--hadoop-mapred-home", "/opt/module/hadoop-3.1.3"
            };
            String[] expandArguments = OptionsFileUtil.expandArguments(args);
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://bigData101:8020");
            Sqoop.runTool(expandArguments, conf);
            loadData(targetDir);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            hiveDestory();
        }
    }

    // hive导入数据
    public static void loadData(String hdfsPath) {
        try {
            hiveInit();
            String sql = "load data inpath '" + hdfsPath + "' overwrite into table emp6";
            System.out.println("Running: " + sql);
            stmt.execute(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            hiveDestory();
        }
    }

    // hive关联查询
    public static void insertSelect() {
        try {
            hiveInit();
            String sql = "insert into table ads_order_by_province select '2021-08-30' dt,bp.id,bp.name,bp.area_code,bp.iso_code,bp.iso_3166_2,count(*) order_count,sum(oi.final_amount) order_amount from dwd_order_info oi left join dim_base_province bp on oi.province_id=bp.id group by bp.id,bp.name,bp.area_code,bp.iso_code,bp.iso_3166_2";
            System.out.println("Running: " + sql);
            stmt.execute(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            hiveDestory();
        }
    }

    // 释放资源
    public static void hiveDestory() {
        try {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
