package com.zhc.flink.course05

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class SinkToMySQL extends RichSinkFunction[Student] {

  var connect: Connection = null
  var pstm: PreparedStatement = null

  def getConnection = {

    Class.forName("com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://localhost:3306/imooc?useSSL=false"

    connect = DriverManager.getConnection(url, "root", "root")
    connect
  }

  /**
    * 在open 方法中创建连接
    *
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {

    val conn = getConnection
    val sql = "insert into student(id,name,age) values (?,?,?) ON DUPLICATE KEY UPDATE name=?,age=?"

    pstm = conn.prepareStatement(sql)
    println("open")
  }

  /**
    * 每条记录插入时调用一次
    *
    * @param value
    * @param context
    */
  override def invoke(value: Student, context: SinkFunction.Context[_]): Unit = {

    pstm.setInt(1, value.id)
    pstm.setString(2, value.name)
    pstm.setInt(3, value.age)
    pstm.setString(4, value.name)
    pstm.setInt(5, value.age)

    pstm.executeUpdate()
    println("invoke~~~~~~~~~~~")
  }

  /**
    * 关闭资源
    */
  override def close(): Unit = {

    if (pstm != null) {
      pstm.close()
    }
    if (connect != null) {
      connect.close()
    }
  }
}