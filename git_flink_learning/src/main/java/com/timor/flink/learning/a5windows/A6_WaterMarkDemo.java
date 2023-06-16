package com.timor.flink.learning.a5windows;

import com.timor.flink.learning.dao.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Title: A6_WaterMarkDemo
 * @Package: com.timor.flink.learning.a5windows
 * @Description:
 * @Author: XXX
 * @Date: 2023/6/1 17:37
 * @Version:1.0
 */
public class A6_WaterMarkDemo {

    public static void main(String[] args) throws Exception {

        //一直有个bug就是事件时间增加，但是process函数不调用

        /*bug原因找到了,因为没设置并行度导致默认是8个线程，而水位线必须8个流里的数据都有数据并且事件时间更新到10s时才触发process执行
        *因为之前输入的key都是a,b导致其他线程没数据，导致其他线程水位线时间不更新，所以尽管a,1000但是还是不触发process窗口关闭
        * 后面设置并行度为2,就好了，不过必须a,b 2个事件时间都超过10
        * 为了避免这种情况可以设置空闲时间等待.withIdleness(Duration.ofSeconds(10))
        * //空闲等待10s，即当10s内其他分区没有数据更新事件时间是，等10s，按最大的时间时间同步到其他没数据的分区
        * */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> ds = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<WaterSensor> map = ds.map(s -> {
            String[] split = s.split(",");
            return new WaterSensor(split[0], Long.valueOf(split[1]), 5);
        })
        .setParallelism(2);

        // 定义Watermark策略,<WaterSensor>forBoundedOutOfOrderness,必须加范型在前面，不然后面调用withTimestampAssigner方法会爆红
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                //  指定watermark生成：乱序的，等待3s
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withIdleness(Duration.ofSeconds(10))  //空闲等待10s，即当10s内其他分区没有数据更新事件时间是，等3s同步水位线
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        System.out.println("调用了获取时间时间方法:" + element.toString()+"recordTS:"+recordTimestamp);
                        //默认单位是毫秒
                        long eventTime = element.getTs() * 1000L;
                        System.out.println(eventTime);
                        return eventTime;
                    }
                });

        //设置watermark策略,只能在SingleOutputStreamOperator上设置水位线策略,wiondow的DS没有assignTimestampsAndWatermarks方法
        //keyStream也可以assignTimeStamp不过返回的是SingleOutputStrea,这样无法调用wiondow方法了，所以只能在map时设置水位线规则
        SingleOutputStreamOperator<WaterSensor> assginDS = map.assignTimestampsAndWatermarks(watermarkStrategy);

        KeyedStream<WaterSensor, String> keyBy = assginDS.keyBy(sensor -> sensor.getId());

        //和之前窗后区别用的是TumblingEventTimeWindows，之前的是TumblingProcessingTimeWindows
        WindowedStream<WaterSensor, String, TimeWindow> window = keyBy.window(TumblingEventTimeWindows.of(Time.seconds(10)));


        /*
         * 窗口实际关闭延迟为forBoundedOutOfOrderness(Duration.ofSeconds(2)) + allowedLateness(Time.seconds(2))
         * 一个是水位线延迟更新，一个是窗口延迟关闭，
         * 区别：当事件时间到12s时，水位线更新到10，这时窗口触发执行代码print操作，因为设置了allowedLateness，窗口在不会关闭，
         * 而是当有迟到数据是，继续追加到第一个10s窗口，来一条追加一条执行prin。当时间到14时，窗口才彻底关闭
         * */
        SingleOutputStreamOperator<String> process = window
                .allowedLateness(Time.seconds(2)) //窗口延迟2S关闭
                .process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context,
                                        Iterable<WaterSensor> elements, Collector<String> out) throws Exception {

                        System.out.println( "当前水位线"+  context.currentWatermark());
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        String startfm = DateFormatUtils.format(start, "HH:mm:ss.SSS");
                        String endfm = DateFormatUtils.format(end, "HH:mm:ss.SSS");
                        System.out.println("调用了process方法");

                        out.collect("起始时间:" + startfm + "--" + "终止时间" + endfm +":");
                        out.collect( "collect收集到的"+ elements.toString());


                    }
                }

        );

        process.print();
        env.execute();


    }


}
