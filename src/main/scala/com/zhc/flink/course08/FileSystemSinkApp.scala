package com.zhc.flink.course08

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

object FileSystemSinkApp {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.socketTextStream("localhost", 9999)

    data.print().setParallelism(1)

    val fileOutputPath = "file:///Users/zhaohaichao/workspace/javaspace/flink-train/data/output"

    val sink = new BucketingSink[String](fileOutputPath)
    sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd--HHmm"))
    sink.setWriter(new StringWriter())
    //    sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    sink.setBatchRolloverInterval(20 * 1000); // this is 20 mins

    data.addSink(sink)

    env.execute("FileSystemSinkApp")

  }

}
