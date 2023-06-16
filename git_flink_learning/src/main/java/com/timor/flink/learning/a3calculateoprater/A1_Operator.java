package com.timor.flink.learning.a3calculateoprater;

import com.timor.flink.learning.dao.WaterSensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @Title: A1_Operator
 * @Package: com.timor.flink.learning.a3calculateoprater
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/29 11:11
 * @Version:1.0
 */
public class A1_Operator {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> ds = env.fromElements("a,a", "b,b", "a,a");

        //map
        SingleOutputStreamOperator<WaterSensor> map = ds.map( (str) -> new WaterSensor(str, 1L, 10));
        map.print();
        System.out.println("-------上面是map---------");

        ArrayList<String>  arr = new ArrayList<>();
        arr.add("a,a");

        //filter
        SingleOutputStreamOperator<WaterSensor> filter = map.filter((WaterSensor sensor) -> arr.contains(sensor.id));
        filter.print();

        //flatmap  1进n出
        //这里报错了因为lamda有个类型擦除，所以即使写了范型也擦除后也相当于没写，所以要手动指定告诉最后返回是什么类型
        SingleOutputStreamOperator<String> flatMap = map.flatMap( ( WaterSensor sen, Collector<String> col) -> {
            for (String str : sen.id.split(",")) col.collect(str);
        }).returns(Types.STRING);
        flatMap.print();


        //keyby是重分区，按返回值key去重新分区。传入一个对象，返回一个key根据返回值来判断2个是否相同来分区
        KeyedStream<WaterSensor, String> keyBy = map.keyBy((WaterSensor sensor) -> sensor.id);


        //sum,max,reduce





        //打印结果很奇怪,env的实际代码是在exute执行，前面的分割线先执行了
        env.execute();

    }

}
