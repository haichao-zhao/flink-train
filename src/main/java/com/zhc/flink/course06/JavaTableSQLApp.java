package com.zhc.flink.course06;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

public class JavaTableSQLApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String filePath = "file:///Users/zhaohaichao/workspace/javaspace/flink-train/data/sales.csv";

        DataSource<SaleLog> csv = env.readCsvFile(filePath)
                .ignoreFirstLine()
                .pojoType(SaleLog.class, "transactionId", "customerId", "itemId", "amountPaid");

        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        tableEnv.registerDataSet("sales",csv);

        Table result = tableEnv.sqlQuery("select customerId,count(1) cnt from sales group by customerId");

        tableEnv.toDataSet(result, Row.class).print();

    }

    public static class SaleLog {

        public String transactionId;
        public String customerId;
        public String itemId;
        public Double amountPaid;
    }
}
