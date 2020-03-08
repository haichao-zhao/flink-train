package com.zhc.flink.course02


import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingWCScalaApp {

  def main(args: Array[String]): Unit = {

    val input = "file:///Users/zhaohaichao/workspace/javaspace/flink-train/data/hello.txt"

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val value: DataStream[String] = env.socketTextStream("localhost", 9999)

    value.flatMap(x => {
      x.toLowerCase.split(" ")
    }).map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print()
      .setParallelism(1)//设置并行度

    env.execute("StreamingWCScalaApp")

  }


}
