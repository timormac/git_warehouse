package com.timor.flink.learning.a5windows;

import com.timor.flink.learning.dao.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Title: A4_AggregateAndProcessDemo
 * @Package: com.timor.flink.learning.a5windows
 * @Description:
 * @Author: XXX
 * @Date: 2023/6/1 12:43
 * @Version:1.0
 */
public class A4_AggregateAndProcessDemo {

    public static void main(String[] args) throws Exception {

        //ProcessWindowFunciton全窗口函数，是必须数据全来了之后，先缓存起来，然后等10s窗口结束统一计算
        //aggregate是来一条算一条，多个数据进来只返回一条数据。
        //ProcessWindowFunciton数据来了先不算，先缓存，等到窗口时间到了，把10s内的数据统一计算
        //Process可以获取到上下文
        //两者结合使用
        //还是没想明白区别
        /**
         * 增量聚合 Aggregate + 全窗口 process
         * 1、增量聚合函数处理数据： 来一条计算一条
         * 2、窗口触发时， 增量聚合的结果（只有一条） 传递给 全窗口函数
         * 3、经过全窗口函数的处理包装后，输出
         *
         * 结合两者的优点：
         * 1、增量聚合： 来一条计算一条，存储中间的计算结果，占用的空间少
         * 2、全窗口函数： 可以通过 上下文 实现灵活的功能
         */



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // nc -lk 7777
        DataStreamSource<String> socket = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<WaterSensor> map = socket.map(message -> {
                    String[] split = message.split(",");
                    return new WaterSensor(split[0], 1L, Integer.valueOf(split[1]));
                }
        );
        SingleOutputStreamOperator<WaterSensor> map3 = map.setParallelism(2);
        KeyedStream<WaterSensor, String> keyBy = map3.keyBy(sensor -> sensor.getId());
        WindowedStream<WaterSensor, String, TimeWindow> window = keyBy.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //结合Aggregate和process的优点
        //会将aggregate的result方法的返回类型传递给process作为输入类型
        SingleOutputStreamOperator<String> aggregate = window.aggregate(new InnerAggregate(), new InnerProcessWindow());
        aggregate.print();
        env.execute();

    }

    static  class InnerAggregate implements AggregateFunction<WaterSensor, Tuple2<String, String>, String> {

        @Override
        public Tuple2<String, String> createAccumulator() {
            System.out.println("createAccumulator调用了1次");
            return Tuple2.of("新建createAccumulator的id:", "新建createAccumulator的vc:");
        }

        @Override
        public Tuple2<String, String> add(WaterSensor value, Tuple2<String, String> accumulator) {
            System.out.println("add方法调用了1次");
            accumulator.f0 = accumulator.f0 + value.getId() + ",";
            accumulator.f1 = accumulator.f1 + value.getVc() + ",";
            return accumulator;
        }

        @Override
        public String getResult(Tuple2<String, String> accumulator) {
            System.out.println("result方法调用1次");
            return accumulator.toString();
        }

        @Override
        public Tuple2<String, String> merge(Tuple2<String, String> a, Tuple2<String, String> b) {
            return null;
        }
    }

    static class InnerProcessWindow extends ProcessWindowFunction<String,String,String,TimeWindow>{

        @Override
        public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context,
                            Iterable<String> elements, Collector<String> out) throws Exception {

            long start = context.window().getStart();
            long end = context.window().getEnd();
            String startfm = DateFormatUtils.format(start, "HH:mm:ss.SSS");
            String endfm = DateFormatUtils.format(end, "HH:mm:ss.SSS");
            System.out.println("调用了process方法");

            for (String element : elements) {
                System.out.println("prcess方法传入的elements迭代,结果为:"+element);
            }

            out.collect("起始时间:" + startfm + "--" + "终止时间" + endfm +"，事件个数:"+ elements.spliterator().estimateSize());

        }

    }



}
