package com.zhc.flink.course07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class JavaWindowsApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> data = env.socketTextStream("localhost", 9999);

//        tumblingWindowsFunc(data);

//        windowsReduceFunc(data);
        windowsProcessFunc(data);
        env.execute("JavaWindowsApp");

    }


    private static SingleOutputStreamOperator<Tuple2<String, Integer>> getTestWindowData(DataStreamSource<String> data) {
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

                String[] strings = value.split(" ");
                for (String s : strings) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        });
        return operator;
    }


    private static SingleOutputStreamOperator<Tuple2<Integer, Integer>> getTestWindowsFunctionData(DataStreamSource<String> data) {
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> operator = data.flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) throws Exception {

                String[] strings = value.split(" ");
                for (String s : strings) {
                    out.collect(new Tuple2<>(1, Integer.parseInt(s)));
                }
            }
        });
        return operator;
    }

    //滚动窗口方式处理
    private static void tumblingWindowsFunc(DataStreamSource<String> data) {

        getTestWindowData(data).keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1)
                .print()
                .setParallelism(1);
    }


    //滑动窗口方式处理
    private static void slidingWindowsFunc(DataStreamSource<String> data) {

        getTestWindowData(data).keyBy(0)
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .sum(1)
                .print()
                .setParallelism(1);
    }

    //窗口函数：Reduce
    private static void windowsReduceFunc(DataStreamSource<String> data) {

        getTestWindowsFunctionData(data).keyBy(0)
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                        System.out.println("v1 = [" + v1 + "], v2 = [" + v2 + "]");
                        return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
                    }
                })
                .print()
                .setParallelism(1);
    }


    //窗口函数：Process
    private static void windowsProcessFunc(DataStreamSource<String> data) {

        getTestWindowsFunctionData(data).keyBy(0)
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<Integer, Integer>, Object, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<Integer, Integer>> input, Collector<Object> out) throws Exception {
                        System.out.println("----------------");
                        long count = 0;
                        for (Tuple2<Integer, Integer> in : input) {
                            count++;
                        }
                        out.collect("Window: " + context.window() + "count: " + count);
                    }
                })
                .print()
                .setParallelism(1);
    }

}
