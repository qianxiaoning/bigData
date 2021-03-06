package com.qxn.sqoophivebyjava.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * <简述> 获取配置文件参数 工具类
 * <详细描述> PropertiesUtil
 * 摘自https://blog.csdn.net/sinat_32133675/article/details/78242542
 */
public class PropertiesUtil {
    private static Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);
    // private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static Properties props;

    // Tomcat运行时执行
    // 代码块执行顺序：静态代码块>普通代码块>构造代码块
    // 构造代码块每次都执行，但是静态代码块只执行一次
    static {
        String fileName = "application-third.yml";
        props = new Properties();
        try {
            props.load(new InputStreamReader(PropertiesUtil.class.getClassLoader().getResourceAsStream(fileName),"UTF-8"));
        } catch (IOException e) {
            logger.error("配置文件读取异常",e);
        }
    }

    // 自定义俩个get方法，方便调用工具类读取properties文件的属性
    public static String prop(String key){
        String value= props.getProperty(key.trim());
        if (StringUtils.isBlank(value)){
            return null;
        }
        return value.trim();
    }

    public static String prop(String key, String defaultValue) {
        String value= props.getProperty(key.trim());
        if (StringUtils.isBlank(value)){
            value = defaultValue;
        }
        return value.trim();
    }
}
