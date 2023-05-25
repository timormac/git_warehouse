package com.timor.flink.learning.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Title: A2_WordCountStream
 * @Package: com.timor.flink.learning.demo
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/24 14:01
 * @Version:1.0
 */
public class A2_WordCountStreamBounded {

    public static void main(String[] args) throws Exception {

        //流环境和batch获取环境方式不同，batch是ExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lineDS = env.readTextFile("input_data/wordcount.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDS = lineDS.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

                        String[] splits = s.split(" ");
                        for (int i = 0; i < splits.length; i++) {

                            Tuple2<String, Integer> tuple2 = Tuple2.of(splits[i], 1);
                            //通过采集器向下游发送数据
                            collector.collect(tuple2);

                        }
                    }
                }
        );

        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = tupleDS.keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {

                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                }
        );
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2StringKeyedStream.sum(1);
        sum.print();

        //流环境多一个这个
        env.execute();


    }
}
