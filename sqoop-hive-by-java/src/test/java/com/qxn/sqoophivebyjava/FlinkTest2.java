package com.qxn.sqoophivebyjava;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;


        import org.apache.flink.api.common.serialization.SimpleStringEncoder;
        import org.apache.flink.configuration.Configuration;
        import org.apache.flink.core.fs.Path;
        import org.apache.flink.core.io.SimpleVersionedSerializer;
        import org.apache.flink.streaming.api.CheckpointingMode;
        import org.apache.flink.streaming.api.datastream.DataStream;
        import org.apache.flink.streaming.api.environment.CheckpointConfig;
        import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
        import org.apache.flink.streaming.api.functions.sink.filesystem.*;
        import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
        import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
        import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
        import org.apache.flink.util.Preconditions;

        import java.io.IOException;
        import java.sql.Connection;
        import java.sql.DriverManager;
        import java.sql.SQLException;
        import java.time.ZoneId;
        import java.time.format.DateTimeFormatter;
        import java.util.List;
        import java.util.concurrent.TimeUnit;
public class FlinkTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Person> streamSource = env.addSource(new MyMysqlSource("select * from emp"));
        System.setProperty("HADOOP_USER_NAME", "shonqian");
        // Hive表的HDFS路径
        String outputBasePath = "hdfs://192.168.30.117:8020/user/hive/warehouse/emp4";


        StreamingFileSink<Person> sink = StreamingFileSink.forRowFormat(new Path(outputBasePath), new SimpleStringEncoder<Person>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(30))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 128)
                                .build())
                .withBucketAssigner(new CustomBucketAssigner("yyyyMMdd", ZoneId.of("Asia/Shanghai"), "dt="))
                .withBucketCheckInterval(1)
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartSuffix("--")
                                .withPartPrefix("part")
                                .withPartSuffix(".ext")
                                .build())
                .build();
        streamSource.addSink(sink);
        env.execute();
    }

    /**
     * CREATE TABLE `mysql_hive`  (
     * `id` int(11) NULL DEFAULT NULL,
     * `name` varchar(255) ,
     * `age` int(11) NULL DEFAULT NULL,
     * `money` double NULL DEFAULT NULL,
     * `todate` date NULL DEFAULT NULL,
     * `ts` timestamp NULL DEFAULT NULL
     * ) ;
     */
    public static class MyMysqlSource extends RichSourceFunction<Person> {
        private String sql;
        private Connection conn = null;
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
        }

        @Override
        public void run(SourceContext<Person> sourceContext) throws Exception {
            List<Person> personList = JdbcUtils.queryList(conn, sql, Person.class);
            for (Person person : personList) {
                sourceContext.collect(person);
            }
        }

       /* @Override
        public void run(SourceContext<Test> sourceContext) throws Exception {
            List<Test> personList = JdbcUtils.queryList(conn, sql, Test.class);
            for (Test person : personList) {
                sourceContext.collect(person);
            }

        }*/

        @Override
        public void cancel() {
        }

        @Override
        public void close() throws Exception {
            Thread.sleep(3000);
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static class CustomBucketAssigner implements BucketAssigner<Person, String> {
        private static final long serialVersionUID = 1L;
        private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd";
        private final String formatString;
        private final ZoneId zoneId;
        private final String column;
        private transient DateTimeFormatter dateTimeFormatter;
        public CustomBucketAssigner() {
            this(DEFAULT_FORMAT_STRING);
        }
        public CustomBucketAssigner(String formatString) {
            this(formatString, ZoneId.systemDefault(), "");
        }
        public CustomBucketAssigner(ZoneId zoneId) {
            this(DEFAULT_FORMAT_STRING, zoneId, "");
        }
        public CustomBucketAssigner(String formatString, String column) {
            this(formatString, ZoneId.systemDefault(), column);
        }
        public CustomBucketAssigner(String formatString, ZoneId zoneId) {
            this(formatString, ZoneId.systemDefault(), "");
        }
        public CustomBucketAssigner(String formatString, ZoneId zoneId, String column) {
            this.formatString = Preconditions.checkNotNull(formatString);
            this.zoneId = Preconditions.checkNotNull(zoneId);
            this.column = Preconditions.checkNotNull(column);
        }

        @Override
        public String getBucketId(Person element, Context context) {
            if (dateTimeFormatter == null) {
                dateTimeFormatter = DateTimeFormatter.ofPattern(formatString).withZone(zoneId);
            }
            return "";
        }
        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
        @Override
        public String toString() {
            return "DateTimeBucketAssigner{"
                    + "formatString='"
                    + formatString
                    + '\''
                    + ", zoneId="
                    + zoneId
                    + '}';
        }
    }

    public static  class CustomRollingPolicy implements RollingPolicy<Person,String>{
        @Override
        public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState) throws IOException {
            return false;
        }

        @Override
        public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, Person element) throws IOException {
            return false;
        }

        @Override
        public boolean shouldRollOnProcessingTime(PartFileInfo<String> partFileState, long currentTime) throws IOException {
            return false;
        }
    }
}

