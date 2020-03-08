package com.zhc.flink.course02

import org.apache.flink.api.scala._

object BatchWCScalaApp {

  def main(args: Array[String]): Unit = {

    val input = "file:///Users/zhaohaichao/workspace/javaspace/flink-train/data/hello.txt"

    val env = ExecutionEnvironment.getExecutionEnvironment


    val text: DataSet[String] = env.readTextFile(input)

    text.flatMap(x => {
      x.toLowerCase.split(" ")
    }).map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()

  }

}
