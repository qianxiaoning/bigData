package com.qxn.sqoophivebyjava;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.tool.SqoopTool;
import org.apache.sqoop.util.OptionsFileUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC 操作 Hive（注：JDBC 访问 Hive 前需要先启动HiveServer2）
 */
public class HiveTest {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://192.168.xx.xxx:10000/xxx";
//    private static String url = "jdbc:hive2://192.168.xx.xxx:10000/default";
    private static String user = "shonqian";
    private static String password = "";

    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;

    // 加载驱动、创建连接
    @Before
    public void init() throws Exception {
        try {
            // 创建hive驱动
            Class.forName(driverName);
            conn = DriverManager.getConnection(url,user,password);
            stmt = conn.createStatement();
            // windows环境给与权限
            System.setProperty("HADOOP_USER_NAME","shonqian");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    // mysql2hdfs
    public void mysql2hdfs() {
        try {
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
//                    "--outdir", "~/mySqoopTemp/",
                    "--delete-target-dir",
                    "--target-dir", targetDir,
                    "--fields-terminated-by","\t",
//                    "--hadoop-mapred-home", "/opt/module/hadoop-3.1.3"
            };
            String[] expandArguments = OptionsFileUtil.expandArguments(args);
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://192.168.xx.xxx:8020");
            Sqoop.runTool(expandArguments, conf);
//            loadData(targetDir);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // hive导入数据
    public void loadData(String hdfsPath) {
        String sql = "load data inpath '" + hdfsPath + "' overwrite into table emp6";
        System.out.println("Running: " + sql);
        try {
            stmt.execute(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    // mysqlStructure2hive
    public void mysqlStructure2hive() {
        try {
            String[] args = new String[]{
                    "create-hive-table",
                    "--connect", "jdbc:mysql://192.168.xx.xxx:3306/pra?serverTimezone=Asia/Shanghai",
                    "-username", "root",
                    "-password", "root",
                    "--table", "emp",
                    "--hive-table","xxx",
                    "--fields-terminated-by","\t",
////                    "--hive-table","tianhao.tablename1",
            };
            String[] expandArguments = OptionsFileUtil.expandArguments(args);
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://192.168.xx.xxx:8020");
            Sqoop.runTool(expandArguments, conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 创建表
    @Test
    public void createTable() throws Exception {
        String sql = "create table emp1(\n" +
                "empno int,\n" +
                "ename string,\n" +
                "job string,\n" +
                "mgr int,\n" +
                "hiredate string,\n" +
                "sal double,\n" +
                "comm double,\n" +
                "deptno int\n" +
                ")\n" +
                "row format delimited fields terminated by '\\t'";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 远程操作linux
    @Test
    public void linux() throws Exception {
        // TODO Auto-generated method stub
        String host = "192.168.xx.xxx";
        int port = 22;
        String user = "shonqian";
        String password = "xxx";
        String command = "sqoop-import --connect 'jdbc:mysql://192.168.xx.xxx:3306/mytest' --username root --password root --table user --hive-import --hive-overwrite  --hive-table default.mytest_user --null-string '\\\\N' --null-non-string '\\\\N' -m 1";
        String res = exeCommand(host,port,user,password,command);

        System.out.println(res);
    }
    public static String exeCommand(String host, int port, String user, String password, String command) throws JSchException, IOException {

        JSch jsch = new JSch();
        Session session = jsch.getSession(user, host, port);
        session.setConfig("StrictHostKeyChecking", "no");
        //    java.util.Properties config = new java.util.Properties();
        //   config.put("StrictHostKeyChecking", "no");

        session.setPassword(password);
        session.connect();

        ChannelExec channelExec = (ChannelExec) session.openChannel("exec");
        InputStream in = channelExec.getInputStream();
        channelExec.setCommand(command);
        channelExec.setErrStream(System.err);
        channelExec.connect();
        String out = IOUtils.toString(in, "UTF-8");

        channelExec.disconnect();
        session.disconnect();

        return out;
    }

    // sqoopTest
    @Test
    public void sqoopTest() throws Exception {
        System.setProperty("HADOOP_USER_NAME","shonqian");
        String[] args = new String[]{
                "--connect", "jdbc:mysql://192.168.xx.xxx:3306/pra?serverTimezone=Asia/Shanghai",
                "--driver", "com.mysql.cj.jdbc.Driver",
                "-username", "root",
                "-password", "root",
                "--table", "emp",
                "-m", String.valueOf(1),
                "--target-dir", "/x111"
        };
//        sqoopBean sqoopBean = new sqoopBean();
        String[] expandArguments = OptionsFileUtil.expandArguments(args);
        SqoopTool tool = SqoopTool.getTool("import");
        Configuration conf = new Configuration();
//        conf.addResource(new Path("/opt/module/hadoop-3.1.3/etc/hadoop/core-site.xml"));
//        conf.addResource(new Path("/opt/module/hadoop-3.1.3/etc/hadoop/hdfs-site.xml"));
        conf.set("fs.defaultFS", "hdfs://192.168.xx.xxx:8020");
        Configuration loadPlugins = SqoopTool.loadPlugins(conf);
        Sqoop sqoop = new Sqoop((com.cloudera.sqoop.tool.SqoopTool) tool, loadPlugins);
        Sqoop.runSqoop(sqoop, expandArguments);
//        Configuration config = new Configuration();
//        config.addResource(new Path("/opt/module/hadoop-3.1.3/etc/hadoop/core-site.xml"));
//        config.addResource(new Path("/opt/module/hadoop-3.1.3/etc/hadoop/hdfs-site.xml"));
////        config.set("fs.default.name", "hdfs://192.168.xx.xxx:9870");
//        String[] cmd ={
//                "import",
//                "--connect","jdbc:mysql://192.168.xx.xxx:3306/pra",
//                "--username", "root",
//                "--password", "root",
//                "--hadoop-home", "/opt/module/hadoop-3.1.3",
//                "--table","emp",
////                "--hive-import",
////                "--create-hive-table",
////                "--hive-table","tianhao.emp",
//                "--export-dir","hdfs://192.168.xx.xxx:9870/persons",
////                "-target-dir","/sas",
//                "hdfs://192.168.xx.xxx:8020/user/hive/warehouse",
//                "-m", "1"
////                "--delete-target-dir"
//        };
//        Sqoop.runTool(cmd,config);


//        System.setProperty("HADOOP_USER_NAME","root");
//        String[] args = new String[] {
//                "--connect","jdbc:mysql://192.168.xx.xxx:3306/pra",
//                "--driver","com.mysql.jdbc.Driver",
//                "-username","root",
//                "-password","root",
//                "--table","emp",
//                "-m","1",
//                "--export-dir","hdfs://192.168.xx.xxx:8020/persons",
//                "--input-fields-terminated-by","\t"
//        };
//        String[] expandArguments = OptionsFileUtil.expandArguments(args);
//        SqoopTool tool = SqoopTool.getTool("import");
//        Configuration conf = new Configuration();
//        conf.set("fs.default.name", "hdfs://192.168.xx.xxx:9870");//设置HDFS服务地址
//        Configuration loadPlugins = SqoopTool.loadPlugins(conf);
//        Sqoop sqoop = new Sqoop((com.cloudera.sqoop.tool.SqoopTool) tool, loadPlugins);
//        Sqoop.runSqoop(sqoop, expandArguments);

//        String[] args = new String[]{
//                "--connect", "jdbc:mysql://192.168.xx.xxx:3306/pra?serverTimezone=Asia/Shanghai",
//                "--driver", "com.mysql.cj.jdbc.Driver",
//                "-username", "root",
//                "-password", "root",
//                "--table", "emp",
//                "-m", "1",
//                "--target-dir", "/xx1"
//        };
//        String[] expandArguments = OptionsFileUtil.expandArguments(args);
//        SqoopTool tool = SqoopTool.getTool("import");
//        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://192.168.xx.xxx:8020");
//        Configuration loadPlugins = SqoopTool.loadPlugins(conf);
//        Sqoop sqoop = new Sqoop((com.cloudera.sqoop.tool.SqoopTool) tool, loadPlugins);
//        Sqoop.runSqoop(sqoop, expandArguments);
//        sqoopBean.setI(Sqoop.runSqoop(sqoop, expandArguments));
//        sqoopBean.setTs(new Timestamp(new Date().getTime()));
    }

    // 上传文件到hdfs
    @Test
    public void toHdfs() throws Exception {
        // windows连接linux权限问题
//        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://192.168.xx.xxx:8020/") ,conf,"hadoop01");
        System.setProperty("HADOOP_USER_NAME","shonqian");

        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://192.168.xx.xxx:8020");
//        URI uri = new URI("hdfs://server:8020");
        FileSystem fs = FileSystem.get(uri,conf);
        Path resP = new Path("C:/Users/Administrator/Desktop/order_info.txt");
//        Path resP = new Path("C:/Users/Administrator/Desktop/order_info.xlsx");
//        Path resP = new Path("/home/hdfs/files/test.txt");
        Path destP = new Path("/aaaaaa");
        if(!fs.exists(destP)){
            fs.mkdirs(destP);
        }
        fs.copyFromLocalFile(resP, destP);
        fs.close();
    }

    // 设置开始位置
    @Test
    public void setStart() throws Exception {
        String sql = "alter table dwd_order_info set tblproperties ('skip.header.line.count'='1')";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 加载数据
    @Test
    public void loadData() throws Exception {
        String hdfsPath = "/aaaaaa";
        String sql = "load data inpath '" + hdfsPath + "' overwrite into table dwd_order_info";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // hive关联查询
    @Test
    public void insertSelect() throws Exception {
        String sql = "insert into table ads_order_by_province select '2021-08-30' dt,bp.id,bp.name,bp.area_code,bp.iso_code,bp.iso_3166_2,count(*) order_count,sum(oi.final_amount) order_amount from dwd_order_info oi left join dim_base_province bp on oi.province_id=bp.id group by bp.id,bp.name,bp.area_code,bp.iso_code,bp.iso_3166_2";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 创建数据库
    @Test
    public void createDatabase() throws Exception {
        String sql = "create database hive_jdbc_test";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 更新表字段
    @Test
    public void alterField() throws Exception {
        String sql = "alter table front_name_age replace columns(name1 string,age varchar(100))";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 查询所有数据库
    @Test
    public void showDatabases() throws Exception {
        String sql = "show databases";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        System.out.println(1);
    }

    // 查询所有表
    @Test
    public void showTables() throws Exception {
        String sql = "show tables";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    // 查询表是否存在
    @Test
    public void seeTableIfExist() throws Exception {
        String a = "front_name_age1";
        String sql = "show tables like '" + a + "'";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            String string = rs.getString(1);
            System.out.println("".equals(null));
        }
    }

    // 查看表结构
    @Test
    public void descTable() throws Exception {
        String sql = "desc front_name_age";
//        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        List<Map<String, String>> fieldList = new ArrayList<>();
        while (rs.next()) {
            Map<String, String> fieldMap = new HashMap<>();
            fieldMap.put("name", rs.getString(1));
            fieldMap.put("type", rs.getString(2));
            fieldList.add(fieldMap);
        }
        System.out.println(fieldList);
    }

    // 查询数据
    @Test
    public void selectData() throws Exception {
        String sql = "select * from emp";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        System.out.println("员工编号" + "\t" + "员工姓名" + "\t" + "工作岗位");
        while (rs.next()) {
            System.out.println(rs.getString("empno") + "\t\t" + rs.getString("ename") + "\t\t" + rs.getString("job"));
        }
    }

    // 统计查询（会运行mapreduce作业）
    @Test
    public void countData() throws Exception {
        String sql = "select count(1) from emp";
        System.out.println("Running: " + sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getInt(1) );
        }
    }

    // 删除数据库
    @Test
    public void dropDatabase() throws Exception {
        String sql = "drop database if exists hive_jdbc_test";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 删除数据库表
    @Test
    public void deopTable() throws Exception {
        String sql = "drop table if exists emp";
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }

    // 释放资源
    @After
    public void destory() throws Exception {
        if ( rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

}