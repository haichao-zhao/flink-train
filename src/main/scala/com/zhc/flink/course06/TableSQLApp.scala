package com.zhc.flink.course06

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object TableSQLApp {
  def main(args: Array[String]): Unit = {

    val filePath = "file:///Users/zhaohaichao/workspace/javaspace/flink-train/data/sales.csv"

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val csv = env.readCsvFile[SaleLog](filePath, ignoreFirstLine = true)

//    val saleTable = tableEnv.fromDataSet(saleDataSet)
//    tableEnv.registerTable("sales",saleTable)

    tableEnv.registerDataSet("sales",csv)

    val resultTable = tableEnv.sqlQuery("select customerId,count(1) cnt from sales group by customerId")

    tableEnv.toDataSet[Row](resultTable).print()

  }

  case class SaleLog(transactionId: String,
                     customerId: String,
                     itemId: String,
                     amountPaid: Double)

}
