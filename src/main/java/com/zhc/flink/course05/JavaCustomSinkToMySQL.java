package com.zhc.flink.course05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JavaCustomSinkToMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> data = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<JavaStudent> studentSingleOutputStreamOperator = data.map(new MapFunction<String, JavaStudent>() {
            @Override
            public JavaStudent map(String value) throws Exception {
                String[] split = value.split(",");
                JavaStudent student = new JavaStudent();
                student.setId(Integer.parseInt(split[0]));
                student.setName(split[1]);
                student.setAge(Integer.parseInt(split[2]));
                return student;
            }
        });

        studentSingleOutputStreamOperator.addSink(new JavaSinkToMySQL());

        env.execute("JavaCustomSinkToMySQL");
    }
}
