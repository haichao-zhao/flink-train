package com.zhc.flink.course05

import java.lang

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable.ListBuffer

object DataStreamTransformationApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    filterFunction(env)

    //    unionFunction(env)

    splitSelectFunction(env)
    env.execute("DataStreamTransformationApp")
  }

  def splitSelectFunction(env: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    import scala.collection.JavaConverters._
    val data = env.addSource(new CustomNonParallelSourceFunction)

    val splits = data.split(new OutputSelector[Long] {
      override def select(value: Long) = {

        val list = new ListBuffer[String]()

        if (value % 2 == 0) {
          list += "even"
        } else {
          list += "odd"
        }
        list.asJava
      }
    })

    splits.select("even", "odd").print().setParallelism(1)
  }


  def unionFunction(env: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val data1 = env.addSource(new CustomNonParallelSourceFunction)
    val data2 = env.addSource(new CustomNonParallelSourceFunction)

    data1.union(data2).print().setParallelism(1)
  }


  def filterFunction(env: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val data = env.addSource(new CustomNonParallelSourceFunction)

    data.map(x => {
      println("received: " + x)
      x
    }).filter(_ % 2 == 0).print().setParallelism(1)
  }

}