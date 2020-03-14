package com.zhc.flink.course08

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object KafkaConnectorProducerApp {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint 常用参数
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) //Checkpoint 执行语义
    checkpointConfig.setCheckpointInterval(5000) //Checkpoint 执行间隔
    checkpointConfig.setCheckpointTimeout(10000) //Checkpoint 超时时间
    checkpointConfig.setMaxConcurrentCheckpoints(1) //Checkpoint 最大并行度

    val topic = "kafka_streaming_topic"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val data = env.socketTextStream("localhost", 9999)
    data.print()

    val kafkaSink = new FlinkKafkaProducer[String](topic, new SimpleStringSchema(), properties)

    data.addSink(kafkaSink)

    env.execute("KafkaConnectorProducerApp")

  }

}
