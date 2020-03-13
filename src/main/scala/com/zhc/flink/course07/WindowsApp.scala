package com.zhc.flink.course07


import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

object WindowsApp {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.socketTextStream("localhost", 9999)

    tumblingWindowsFunc(data)

    env.execute("WindowsApp")
  }

  //滚动窗口方式处理
  private def tumblingWindowsFunc(data: DataStream[String]) = {
    data.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print()
  }

  //滑动窗口方式处理
  private def slidingWindowsFunc(data: DataStream[String]) = {
    data.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(10),Time.seconds(5))
      .sum(1)
      .print()
  }
}
