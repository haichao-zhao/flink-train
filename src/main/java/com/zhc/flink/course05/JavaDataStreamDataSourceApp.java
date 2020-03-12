package com.zhc.flink.course05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JavaDataStreamDataSourceApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        socketFunction(env);

//        nonParallelSourceFunction(env);
//        parallelSourceFunction(env);
        richParallelSourceFunction(env);
        env.execute("JavaDataStreamDataSourceApp");

    }

    private static void richParallelSourceFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new JavaCustomRichParallelSourceFunction());
        data.setParallelism(2).print();
    }

    private static void parallelSourceFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new JavaCustomParallelSourceFunction());
        data.setParallelism(2).print();
    }

    private static void nonParallelSourceFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new JavaCustomNonParallelSourceFunction());
        data.print();
    }

    private static void socketFunction(StreamExecutionEnvironment env) {

        DataStreamSource<String> data = env.socketTextStream("localhost", 9999);

        data.print().setParallelism(1);

    }
}
