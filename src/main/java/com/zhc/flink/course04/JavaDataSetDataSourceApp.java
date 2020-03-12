package com.zhc.flink.course04;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.ArrayList;

public class JavaDataSetDataSourceApp {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//        fromCollection(env);

        textFile(env);

    }

    //从集合的方式创建DataSource
    public static void fromCollection(ExecutionEnvironment env) throws Exception {
        ArrayList<Integer> list = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            list.add(i);
        }

        DataSource<Integer> dataSource = env.fromCollection(list);
        dataSource.print();

    }

    //从文件或者文件夹的方式创建DataSource
    public static void textFile(ExecutionEnvironment env) throws Exception {
        String directory = "file:///Users/zhaohaichao/workspace/javaspace/flink-train/data";

//    String file = "file:///Users/zhaohaichao/workspace/javaspace/flink-train/data/hello.txt";

        DataSource<String> dataSource = env.readTextFile(directory);

        dataSource.print();


    }

}
