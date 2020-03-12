package com.zhc.flink.course05


import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object DataStreamDataSourceApp {

  def socketFunction(env: StreamExecutionEnvironment) = {
    val data = env.socketTextStream("localhost", 9999)
    data.print().setParallelism(1)
  }

  def nonParallelSourceFunction(env: StreamExecutionEnvironment) = {
    val data = env.addSource(new CustomNonParallelSourceFunction)
      .setParallelism(1)
    data.print()
  }

  def parallelSourceFunction(env: StreamExecutionEnvironment) = {
    val data = env.addSource(new CustomParallelSourceFunction)
      .setParallelism(2)
    data.print()
  }

  def richParallelSourceFunction(env: StreamExecutionEnvironment) = {
    val data = env.addSource(new CustomRichParallelSourceFunction)
      .setParallelism(2)
    data.print()
  }

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    socketFunction(env)
    //    nonParallelSourceFunction(env)
    //    parallelSourceFunction(env)
    richParallelSourceFunction(env)
    env.execute("DataStreamDataSourceApp")


  }

}
