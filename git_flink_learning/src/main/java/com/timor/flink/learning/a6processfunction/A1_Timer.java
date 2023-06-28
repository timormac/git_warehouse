package com.timor.flink.learning.a6processfunction;

import com.timor.flink.learning.dao.WaterSensor;
import com.timor.flink.learning.tools.UdfWaterSensorMap;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Title: A1_Timer
 * @Package: com.timor.flink.learning.a6processfunction
 * @Description:
 * @Author: XXX
 * @Date: 2023/6/27 12:21
 * @Version:1.0
 */
public class A1_Timer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> map = env
                .socketTextStream("localhost", 7777)
                .map(new UdfWaterSensorMap());



        WatermarkStrategy<WaterSensor> watermark = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });

        SingleOutputStreamOperator<WaterSensor> mapWithWatermark = map.assignTimestampsAndWatermarks(watermark);

        KeyedStream<WaterSensor, String> keyByWithWatermark = mapWithWatermark.keyBy((sensor) -> sensor.getId());


        /**
         * TODO 定时器
         * 1、keyed才有
         * 2、事件时间定时器，通过watermark来触发的
         *    watermark >= 注册的时间
         *    注意： watermark = 当前最大事件时间 - 等待时间 -1ms， 因为 -1ms，所以会推迟一条数据
         *        比如， 5s的定时器，
         *        如果 等待=3s， watermark = 8s - 3s -1ms = 4999ms,不会触发5s的定时器
         *        需要 watermark = 9s -3s -1ms = 5999ms ，才能去触发 5s的定时器
         * 3、在process中获取当前watermark，显示的是上一次的watermark
         *    =》因为process还没接收到这条数据对应生成的新watermark
         */

        //process中的可调用方法
        SingleOutputStreamOperator<String> process = keyByWithWatermark.process(

                new KeyedProcessFunction<String, WaterSensor, String>() {

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx,
                                               Collector<String> out) throws Exception {
                        //获取当前数据中的事件时间,WatermarkStrategy规则的时间
                        Long timestamp = ctx.timestamp();

                        TimerService timerService = ctx.timerService();

                        //注册定时器:事件时间
                        timerService.registerEventTimeTimer(5000L);
                        System.out.println("当前时间：" + timestamp + "|" + value.toString() + ",注册了一个5s之后的定时器");
                        out.collect("out collect收集的数据");

                        //注册定时器处理时间,虽然流定义了时间事件，process里也可以调用处理时间定时器
                        long currents = timerService.currentProcessingTime();
                        //和event不同，这里要指定具体时间
                        //timerService.registerProcessingTimeTimer( currents+ 5000L);
                        //System.out.println("当前时间：" + currents + "|" + value.toString() + ",注册了一个5s之后的定时器");

                        //删除定时器处理时间
                        //timerService.deleteProcessingTimeTimer();

                        //删除定时器事件时间
                        //timerService.deleteEventTimeTimer();

                        //获取当前的watermark的推进
                        //timerService.currentWatermark();

                        //获取当前处理时间:就是系统时间
                        //timerService.currentProcessingTime();

                    }


                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        String currentKey = ctx.getCurrentKey();
                        System.out.println("key=" + currentKey + "现在时间是" + timestamp + "定时器触发");

                        out.collect("定时器触发,collect收集的数据");
                    }
                }

        );
        process.print();

        env.execute();


    }

}
