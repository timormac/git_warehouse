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


        /*
        *AggregateAndProcess和Agg与process的区别
        *process会把所有数据先缓存，当窗口结束时统一计算，并且能拿到context
        *aggregate来一条算一条，是按key聚合最后每种ke，只返回一条数据
        *2者结合,先执行aggregate，来一条算一条，然后agg将result方法的结果传递给process的iterator
        * 所以process中的Iterable<String> elements只有一条数据,然后通过out.collect来输出
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

    //第一个范型是In的类型，与Aggregate结合就是Aggregate的输出类型，第二范型是输出类型
    //第三个范型是keyBy时的key的类型，第4个范型是窗口类型
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
