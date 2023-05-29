package com.timor.flink.learning.demo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Title: A4_WordCountParallel
 * @Package: com.timor.flink.learning.demo
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/26 15:24
 * @Version:1.0
 */
public class A4_WordCountParallel {


    public static void main(String[] args) throws Exception {

    //  这个是idea执行创建一个有webUI的en,登陆localhost:8081能看到flinkweb界面，pom文件还要导入flink-runtime-web依赖
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //env.setParallelism(3);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(
                        (String value, Collector<Tuple2<String, Integer>> col) -> {
                            String[] spilts = value.split(" ");
                            for (String s : spilts) {
                                col.collect(Tuple2.of(s, 1));
                            }
                        }
                ).setParallelism(2)
                //设置flatmap这一个算子并行度为2
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(value -> value.f0)
                //sum算子没有指定并行度，idea执行默认跟本机核数相同
                .sum(1);


        sum.print();
        env.execute();


        //nc -lk 7777
    }

}
