package com.zhc.flink.course02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class BatchWCJavaApp {
    public static void main(String[] args) throws Exception {

        //"file:///Users/zhaohaichao/workspace/javaspace/flink-train/data/hello.txt"
        String input = "file:///Users/zhaohaichao/workspace/javaspace/flink-train/data/hello.txt";

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = env.readTextFile(input);

//        source.print();

        source.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> out) {

                        String[] tokens = s.toLowerCase().split(" ");

                        for (String token : tokens) {
                            if (token.length() > 0) {
                                out.collect(new Tuple2<>(token, 1));
                            }
                        }
                    }
                }).groupBy(0)
                .sum(1).print();


//        env.execute();
    }
}
