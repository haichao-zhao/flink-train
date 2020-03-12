package com.zhc.flink.course05

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object CustomSinkToMySQL {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.socketTextStream("localhost",9999)

    val ds: DataStream[Student] = data.map(x => {
      val strings = x.split(",")
      Student(strings(0).toInt, strings(1), strings(2).toInt)
    })

    ds.addSink(new SinkToMySQL)

    env.execute("CustomSinkToMySQL")

  }
}
