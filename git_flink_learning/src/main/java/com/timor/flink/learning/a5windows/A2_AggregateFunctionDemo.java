package com.timor.flink.learning.a5windows;

import com.timor.flink.learning.dao.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @Title: A2_WinodwOperator
 * @Package: com.timor.flink.learning.a5windows
 * @Description:
 * @Author: XXX
 * @Date: 2023/6/1 11:55
 * @Version:1.0
 */
public class A2_AggregateFunctionDemo {

    public static void main(String[] args) throws Exception {

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

        //来一条计算一次，不过不会输出，当窗口到时间了才输出,可以理解成多进1出。
        //返回的是一个SingleOutputStreamOperator,不过这里有个问题，就是我上面设置Parallism是2，下面查看并行度为8
        SingleOutputStreamOperator<String> aggregate = window.aggregate(

                new AggregateFunction<WaterSensor, Tuple2<String, String>, String>() {

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

                //merge方法只有会话窗口才能用到,其他的用不到
                @Override
                public Tuple2<String, String> merge(Tuple2<String, String> a, Tuple2<String, String> b) {
                    System.out.println( "merge方法调用一次");
                    return null;
                }
            }
        );

        System.out.println(aggregate.getParallelism());

        DataStreamSink<String> print = aggregate.print();

        env.execute();



    }
}
