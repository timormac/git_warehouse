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
 * @Title: A1_RollingWindow
 * @Package: com.timor.flink.learning.a5windows
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/31 09:35
 * @Version:1.0
 */
public class A1_WindowApi {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socket = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<WaterSensor> map = socket.map(message -> {
                    String[] split = message.split(",");
                    return new WaterSensor(split[0], 1L, Integer.valueOf(split[1]));
                }
        );
        SingleOutputStreamOperator<WaterSensor> map3 = map.setParallelism(2);


        // 1.1 没有keyby的窗口: 窗口内的 所有数据 进入同一个 子任务，并行度只能为1
//         1.2 有keyby的窗口: 每个key上都定义了一组窗口，各自独立地进行统计计算
        KeyedStream<WaterSensor, String> keyBy = map3.keyBy(sensor -> sensor.getId());


        //如果没有keyby,只有windowAll方法，得到一个AllWindowedStream，切并行度强行为1
        AllWindowedStream<WaterSensor, TimeWindow> allwindow = map3.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));


        //winodow()的参数需要WindowAssigner对象，通过control H查看实现类有:
        // ProcessingTimeSessionWindows,SlidingProcessingTimeWindows,TumblingProcessingTimeWindows等
        // 然后发现ProcessingTimeSessionWindows的构造器是私有的,看到of方法会调用构造器生成对象
        WindowedStream<WaterSensor, String, TimeWindow> window = keyBy.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));


        // 基于时间的
//        keyBy.window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 滚动窗口，窗口长度10s
//        keyBy.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2))) // 滑动窗口，窗口长度10s，滑动步长2s
//        keyBy.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))) // 会话窗口，超时间隔5s

        // 基于计数的
//        keyBy.countWindow(5)  // 滚动窗口，窗口长度=5个元素
//        keyBy.countWindow(5,2) // 滑动窗口，窗口长度=5个元素，滑动步长=2个元素
//        keyBy.window(GlobalWindows.create())  // 全局窗口，计数窗口的底层就是用的这个，需要自定义的时候才会用

        //来一条计算一次，不过不会输出，当窗口到时间了才输出,可以理解成多进1出。
        //返回的是一个SingleOutputStreamOperator,不过这里有个问题，就是我上面设置Parallism是2，下面查看并行度为8
        SingleOutputStreamOperator<String> aggregate = window.aggregate(new AggregateFunction<WaterSensor, Tuple2<String, String>, String>() {
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

                            //Tuple2<String, String> t2 = new Tuple2<>( accumulator.f0+value.getId()+","  ,accumulator.f1+value.vc+",");

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
