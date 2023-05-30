package com.timor.flink.learning.a3calculateoprater;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Title: A6_StreamConnect
 * @Package: com.timor.flink.learning.a3calculateoprater
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/30 10:32
 * @Version:1.0
 */
public class A6_StreamConnect {

    public static void main(String[] args) throws Exception {

        //connect和union区别,union只能是同类型合流，connect是不同类型输出成同类型的合流
        //并且connect 里面是2个方法,所以才能写2流join的逻辑

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.fromElements("a", "a");
        DataStreamSource<Integer> ds2 = env.fromElements(1, 2, 3);

        //connect流返回的是个connectedStream,需要map等算子后才能返回一个DataStream才能做其他操作，
        ConnectedStreams<String, Integer> connect = ds1.connect(ds2);

        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value;
            }

            @Override
            public String map2(Integer value) throws Exception {
                return String.valueOf(value);
            }
        });
        map.print();


        env.execute();


    }

}
