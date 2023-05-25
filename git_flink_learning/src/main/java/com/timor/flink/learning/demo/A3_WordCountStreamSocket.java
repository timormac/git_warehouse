package com.timor.flink.learning.demo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Title: A3_WordCountSteamSocket
 * @Package: com.timor.flink.learning.demo
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/25 09:40
 * @Version:1.0
 */
public class A3_WordCountStreamSocket {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.socketTextStream("project1", 7777);

        //这里会报错：The generic type parameters of 'Collector' are missing.
        //In many cases lambda methods don't provide enough information for automatic type extraction when Java generics are involved
        //因为lamda表达式无法推断出类型，因为范型最后会擦除，所以最后要手动指定范型，好像是flnk的东西
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(
                (String value, Collector<Tuple2<String, Integer>> col) -> {
                    String[] spilts = value.split(" ");
                    for (String s : spilts) {
                        col.collect(Tuple2.of(s, 1));
                    }
                }
        ).returns(Types.TUPLE(Types.STRING,Types.INT)) //指定范型，不然会报错，因为lamda表达式的问题
        .keyBy(value -> value.f0)
        .sum(1);

        env.execute();


        //nc -lk 7777
    }


}
