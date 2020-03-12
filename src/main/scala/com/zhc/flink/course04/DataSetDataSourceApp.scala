package com.zhc.flink.course04

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object DataSetDataSourceApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //    fromCollection(env)

    //    textFile(env)
    //    csvFile(env)

    readRecursiveFiles(env)
  }

  //从集合的方式创建DataSet
  def fromCollection(env: ExecutionEnvironment): Unit = {


    val data = 1 to 10
    val text: DataSet[Int] = env.fromCollection(data)
    text.print()

  }

  //从文件或者文件夹的方式创建DataSet
  def textFile(env: ExecutionEnvironment): Unit = {
    val directory = "file:///Users/zhaohaichao/workspace/javaspace/flink-train/data"

    //    val file = "file:///Users/zhaohaichao/workspace/javaspace/flink-train/data/hello.txt"

    val text: DataSet[String] = env.readTextFile(directory)
    text.print()
  }

  //从csv文件创建DataSet
  def csvFile(env: ExecutionEnvironment): Unit = {

    case class People(name: String, age: Int)

    val file = "file:///Users/zhaohaichao/workspace/javaspace/flink-train/data/people.csv"
    //    val text = env.readCsvFile[(String,Int,String)](file,ignoreFirstLine = true)

    //includedFields参数中Array中的index顺序必须从小到大
    //val text: DataSet[(Int, String)] = env.readCsvFile[(Int, String)](file, ignoreFirstLine = true, includedFields = Array(1, 2))

    //case class 方式
    //val text: DataSet[People] = env.readCsvFile[People](file,ignoreFirstLine = true)

    //pojo 方式
    val text: DataSet[PeoplePojo] = env.readCsvFile[PeoplePojo](file, ignoreFirstLine = true, pojoFields = Array("name", "age", "job"))

    text.print()
  }

  //从递归文件夹的内容创建DataSet
  def readRecursiveFiles(env: ExecutionEnvironment): Unit = {
    val directory = "file:///Users/zhaohaichao/workspace/javaspace/flink-train/src/main/java/com/zhc/flink"

    // create a configuration object
    val parameters = new Configuration

    // set the recursive enumeration parameter
    parameters.setBoolean("recursive.file.enumeration", true)

    val text: DataSet[String] = env.readTextFile(directory).withParameters(parameters)
    text.print()
  }

}
