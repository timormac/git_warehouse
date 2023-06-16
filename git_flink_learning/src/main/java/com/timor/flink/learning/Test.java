package com.timor.flink.learning;

import com.timor.flink.learning.dao.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Title: Test
 * @Package: com.timor.flink.learning
 * @Description:
 * @Author: XXX
 * @Date: 2023/6/1 18:40
 * @Version:1.0
 */
public class Test {

    public static void main(String[] args) throws Exception {


            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
            /**
             * 演示watermark多并行度下的传递
             * 1、接收到上游多个，取最小
             * 2、往下游多个发送， 广播
             */
            env.setParallelism(2);
            SingleOutputStreamOperator<WaterSensor> sensorDS = env
                    .socketTextStream("localhost", 7777)
                    .map(s -> {
                        String[] split = s.split(",");
                        return new WaterSensor(split[0], Long.valueOf(split[1]), 5);
                    });

            // TODO 1.定义Watermark策略
            WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                    // 1.1 指定watermark生成：乱序的，等待3s
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    // 1.2 指定 时间戳分配器，从数据中提取
                    .withTimestampAssigner(
                            (element, recordTimestamp) -> {
                                // 返回的时间戳，要 毫秒
                                System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                                return element.getTs() * 1000L;
                            });

            // TODO 2. 指定 watermark策略
            SingleOutputStreamOperator<WaterSensor> sensorDSwithWatermark = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);


            sensorDSwithWatermark.keyBy(sensor -> sensor.getId())
                    // TODO 3.使用 事件时间语义 的窗口
                    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                    .process(
                            new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {

                                @Override
                                public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                    long startTs = context.window().getStart();
                                    long endTs = context.window().getEnd();
                                    String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                                    String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                                    long count = elements.spliterator().estimateSize();

                                    for (WaterSensor element : elements) {

                                        out.collect( element.toString() + "|||"  );

                                    }

                                }
                            }
                    )
                    .print();

            env.execute();


    }
}
