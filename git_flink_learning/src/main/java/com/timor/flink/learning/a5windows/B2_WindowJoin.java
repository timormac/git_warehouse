package com.timor.flink.learning.a5windows;

import com.timor.flink.learning.dao.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @Title: B2_WindowJoin
 * @Package: com.timor.flink.learning.a5windows
 * @Description:
 * @Author: XXX
 * @Date: 2023/6/22 15:12
 * @Version:1.0
 */
public class B2_WindowJoin {

    public static void main(String[] args) throws Exception {
        //winodow join 和 stream的join区别，就是流的join不同于窗口，窗口join是每个相同时间窗口之间进行join
        //而stream的join我们要自定义一个容器存流的数据


        //应该是一个env监听两个socket窗口不允许，创建了2个env还是不行
        //更改代码后还是有异常Caused by: java.lang.ArrayIndexOutOfBoundsException: -2147483648
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        env1.setParallelism(2);


        DataStreamSource<String> ds1 = env1.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<WaterSensor> waterDs1 = ds1.map(str -> {
            String[] split = str.split(",");
            return new WaterSensor(split[0], Long.valueOf(split[1]) * 1000L, 111);
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))//水位线延迟2s更新
                        .withIdleness(Duration.ofSeconds(6))//空闲等待时间6s,到6s自动更新水位线
                        .withTimestampAssigner((sensor, time) ->{
                            System.out.println( "ds1获取事件时间"+sensor.toString());
                           return sensor.getTs();
                        })
        );
        KeyedStream<WaterSensor, String> keyBy1 = waterDs1.keyBy(sensor -> sensor.getId());

        DataStreamSource<String> ds2 = env1.fromElements(
                "a,1",
                "b,1",
                "c,15",
                "d,15"
        );

        KeyedStream<WaterSensor, String> keyBy2 = ds2.map(str -> {
            String[] split = str.split(",");
            return new WaterSensor(split[0], Long.valueOf(split[1]) * 1000L, 111);
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withIdleness(Duration.ofSeconds(6))
                        .withTimestampAssigner((sensor, time) -> {
                            System.out.println("ds2获取事件时间"+sensor.toString());
                             return sensor.getTs();
                        })
        ).keyBy(sensor -> sensor.getId());


        //join 后获得一个JoinedStreams 有2个范型，也就是说2个类型可以不同
        JoinedStreams<WaterSensor, WaterSensor> joinDs = keyBy1.join(keyBy2);

        DataStream<String> apply = joinDs
                .where(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor waterSensor) throws Exception {
                        System.out.println( "调用where的getkey方法");
                        return waterSensor.getId();
                    }
                })//key1 join key2 where是获取ds1的关联key
                .equalTo(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor waterSensor) throws Exception {
                        System.out.println( "调用equalTo的getKey方法" );
                        return waterSensor.getId();
                    }
                })//获取ds2的关联key
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) //窗口大小是10s
                .allowedLateness(Time.seconds(2)) //窗口延迟2S关闭
                .apply(new JoinFunction<WaterSensor, WaterSensor, String>() {
                    @Override
                    public String join(WaterSensor waterSensor, WaterSensor waterSensor2) throws Exception {
                        System.out.println("调用join方法");
                        return waterSensor.toString() + waterSensor2.toString();
                    }
                });//执行join方法

        apply.print();

        env1.execute();



    }

}
