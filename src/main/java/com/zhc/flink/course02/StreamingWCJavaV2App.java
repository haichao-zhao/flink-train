package com.zhc.flink.course02;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class StreamingWCJavaV2App {
    public static void main(String[] args) throws Exception {


        int port = 0;

        try {
            ParameterTool tool = ParameterTool.fromArgs(args);
            port = tool.getInt("port");
        } catch (Exception e) {
            System.err.println("端口未设置，使用默认端口:9999");
            port = 9999;
        }


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", port);

        source.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {

                        String[] tokens = value.toLowerCase().split(",");

                        for (String token : tokens) {
                            if (token.length() > 0) {
                                out.collect(new Tuple2<>(token, 1));
                            }
                        }
                    }
                }).keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1).print();

        env.execute("StreamingWCJavaApp");

    }
}
