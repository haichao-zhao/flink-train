package com.zhc.flink.project

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable

class MySQLSource extends RichParallelSourceFunction[mutable.HashMap[String, String]] {

  var connection: Connection = null
  var ps: PreparedStatement = null

  override def open(parameters: Configuration): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/imooc?useSSL=false"
    val user = "root"
    val password = "root"

    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, password)

    val sql = "select user_id , domain from user_domain_config"
    ps = connection.prepareStatement(sql)
  }

  override def close(): Unit = {
    if (ps != null) {
      ps.close()
    }
    if (connection != null) {
      connection.close()
    }
  }

  override def run(ctx: SourceFunction.SourceContext[mutable.HashMap[String, String]]) = {

    val hashMap = mutable.HashMap[String, String]()
    val resultSet = ps.executeQuery()

    while (resultSet.next()) {
      val user_id = resultSet.getString("user_id")
      val domain = resultSet.getString("domain")

      hashMap.put(domain,user_id)
    }

    ctx.collect(hashMap)
  }

  override def cancel(): Unit = {}
}
