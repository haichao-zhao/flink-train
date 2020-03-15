package com.zhc.flink.project

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.LoggerFactory

import scala.collection.mutable

object LogAnalysis02 {


  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger("LogAnalysis02")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置程序整体并行度 不能写死 在配置文件中配置
    env.setParallelism(1)

    val topic = "kafka_streaming_topic"
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)

    val data = env.addSource(consumer)

    val logData = data.map(x => {
      val splits = x.split("\t")
      val level = splits(2)
      val timeStr = splits(3)

      var time = 0l
      try {
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        time = format.parse(timeStr).getTime
      } catch {
        case e: Exception => {
          logger.error(s"time parse error : $timeStr", e.getMessage)
        }
      }

      val domain = splits(5)
      val traffic = splits(6).toLong

      (level, time, domain, traffic)
    }).filter(_._2 != 0).filter(_._1 == "E")
      .map(x => (x._2, x._3, x._4))

    //log
    val resultData = logData.assignTimestampsAndWatermarks(new myWatermark)
      .keyBy(1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(new WindowFunction[(Long, String, Long), (String, String, Long), Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[(String, String, Long)]): Unit = {

          val domain = key.getField(0).toString
          var sum = 0l
          var time = ""

          val iterator = input.iterator

          iterator.foreach(x => {
            val traffic = x._3
            sum += traffic

            if (!iterator.hasNext) {
              val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
              time = sdf.format(new Date(x._1))
            }
          })

          out.collect(time, domain, sum)
        }
      })

    //MySQL
    val mySqlData = env.addSource(new MySQLSource)

    val res = resultData.connect(mySqlData)
      .flatMap(new CoFlatMapFunction[(String, String, Long), mutable.HashMap[String, String], ((String, String, Long, String))] {

        var userDomainMap = mutable.HashMap[String, String]()

        override def flatMap1(v: (String, String, Long), out: Collector[(String, String, Long, String)]): Unit = {

          val time = v._1
          val domain = v._2
          val traffic = v._3

//          println(userDomainMap)

          val user_id = userDomainMap.getOrElse(domain, "None")

          out.collect((time, domain, traffic, user_id))

        }

        override def flatMap2(value: mutable.HashMap[String, String], out: Collector[(String, String, Long, String)]): Unit = {
          userDomainMap = value
        }
      })

    res.print()

    env.execute("LogAnalysis02")

  }

  /**
    * 获取ES 的Sink
    *
    * @return
    */
  def getESConnectorSink() = {
    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[(String, String, Long)](
      httpHosts,
      new ElasticsearchSinkFunction[(String, String, Long)] {
        def createIndexRequest(element: (String, String, Long)): IndexRequest = {
          val json = new java.util.HashMap[String, Any]
          json.put("time", element._1)
          json.put("domain", element._2)
          json.put("traffics", element._3)

          val id = element._1 + "-" + element._2

          return Requests.indexRequest()
            .index("cdn")
            .`type`("_doc")
            .id(id)
            .source(json)
        }

        override def process(t: (String, String, Long), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(t))
        }
      }
    )
    esSinkBuilder
  }

}

/**
  * 定义水印
  */
class myWatermark extends AssignerWithPeriodicWatermarks[(Long, String, Long)] {
  val maxOutOfOrderness = 100L // 10 seconds

  var currentMaxTimestamp: Long = _

  override def getCurrentWatermark: Watermark = {
    new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }

  override def extractTimestamp(element: (Long, String, Long), previousElementTimestamp: Long): Long = {
    val timestamp = element._1
    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
    timestamp
  }
}