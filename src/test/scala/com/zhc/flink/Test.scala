package com.zhc.flink

import com.zhc.flink.project.MySQLSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object Test {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.addSource(new MySQLSource).setParallelism(1)
    data.print()

    env.execute("Test")
  }

}
