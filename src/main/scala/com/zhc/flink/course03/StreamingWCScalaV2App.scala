package com.zhc.flink.course03

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingWCScalaV2App {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val value: DataStream[String] = env.socketTextStream("localhost", 9999)

    value.flatMap(x => {
      x.toLowerCase.split(" ")
    }).map(WC(_, 1))
      .keyBy(_.word)
      .timeWindow(Time.seconds(5))
      .sum("count")
      .print()
      .setParallelism(1) //设置并行度

    env.execute("StreamingWCScalaV2App")

  }


  case class WC(word: String, count: Integer)

}
