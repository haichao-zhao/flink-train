package com.zhc.flink.course05;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 自定义MySQL Sink类
 */
public class JavaSinkToMySQL extends RichSinkFunction<JavaStudent> {

    Connection connection;
    PreparedStatement pstm;

    private Connection getConnection() {
        Connection conn = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://localhost:3306/imooc?useSSL=false";

            conn = DriverManager.getConnection(url, "root", "root");

        } catch (Exception e) {
            e.printStackTrace();
        }

        return conn;
    }

    /**
     * 在open 方法中创建连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        connection = getConnection();

        String sql = "insert into student(id,name,age) values (?,?,?) ON DUPLICATE KEY UPDATE name=?,age=?";

        pstm = connection.prepareStatement(sql);
        System.out.println("open");
    }

    /**
     * 每条记录插入时调用一次
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(JavaStudent value, Context context) throws Exception {

        pstm.setInt(1, value.getId());
        pstm.setString(2, value.getName());
        pstm.setInt(3, value.getAge());
        pstm.setString(4, value.getName());
        pstm.setInt(5, value.getAge());

        pstm.executeUpdate();
    }


    /**
     * 关闭资源
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (pstm != null) {
            pstm.close();
        }

        if (connection != null) {
            connection.close();
        }
    }
}
