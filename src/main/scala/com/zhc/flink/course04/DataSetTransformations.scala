package com.zhc.flink.course04

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object DataSetTransformations {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //    mapFunc(env)
    filterFunc(env)
  }

  def mapFunc(env: ExecutionEnvironment): Unit = {

    val data: DataSet[Int] = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9))

    data.map((_, 1)).print()


  }

  def filterFunc(env: ExecutionEnvironment): Unit = {
    val data: DataSet[Int] = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9))

    data.map((_, 1)).filter(_._1 > 5).print()
  }

}
