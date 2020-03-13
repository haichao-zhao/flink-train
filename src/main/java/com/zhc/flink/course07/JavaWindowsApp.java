package com.zhc.flink.course07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class JavaWindowsApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> data = env.socketTextStream("localhost", 9999);

        data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

                String[] strings = value.split(" ");
                for (String s : strings) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1)
                .print()
                .setParallelism(1);


        env.execute("JavaWindowsApp");

    }

}
