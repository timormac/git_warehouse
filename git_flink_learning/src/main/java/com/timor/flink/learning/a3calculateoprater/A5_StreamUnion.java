package com.timor.flink.learning.a3calculateoprater;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Title: A5_StreamUnionConnect
 * @Package: com.timor.flink.learning.a3calculateoprater
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/30 09:35
 * @Version:1.0
 */
public class A5_StreamUnion {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> ds1 = env.fromElements(11, 22, 33);
        DataStreamSource<Integer> ds2 = env.fromElements(111, 222, 333);
        DataStreamSource<Integer> ds3 = env.fromElements(111, 222, 333);
        DataStreamSource<String> ds4 = env.fromElements("111", "222", "333");
        //union方法参数可多条,返回的是个DataStream
        DataStream<Integer> union = ds1.union(ds2, ds3);
        //union.print();

        env.execute();

    }

}
