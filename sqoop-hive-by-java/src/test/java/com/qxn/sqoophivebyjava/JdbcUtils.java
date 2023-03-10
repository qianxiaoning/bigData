package com.qxn.sqoophivebyjava;
import org.apache.commons.beanutils.BeanUtils;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtils {
    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {
        //创建集合用于存放查询结果数据
        ArrayList<T> resultList = new ArrayList<>();
        //预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);
        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        //解析结果集resultSet
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            //创建泛型对象
            T t = clz.newInstance();
            //给泛型对象赋值
            for (int i = 1; i <= columnCount; i++) {
                //获取列名
                String columnName = metaData.getColumnName(i);
                //获取列值
                Object value = resultSet.getObject(i);
                BeanUtils.setProperty(t,columnName,value);
            }
            //将该对象添加至集合
            resultList.add(t);

        }
        preparedStatement.close();
        resultSet.close();
        //返回结果集合
        return resultList;

    }

}

