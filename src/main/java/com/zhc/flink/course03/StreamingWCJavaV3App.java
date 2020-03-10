package com.zhc.flink.course03;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class StreamingWCJavaV3App {
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
                new FlatMapFunction<String, WC>() {
                    @Override
                    public void flatMap(String value, Collector<WC> out) {

                        String[] tokens = value.split(",");

                        for (String token : tokens) {
                            if (token.length() > 0) {
                                WC wc = new WC(token, 1);
                                out.collect(wc);
                            }
                        }
                    }
                }).keyBy("word")
                .timeWindow(Time.seconds(5))
                .sum("count").print()
                .setParallelism(1);

        env.execute("StreamingWCJavaV3App");

    }

    public static class WC {
        private String word;
        private Integer count;

        public WC(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

}
