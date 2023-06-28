package com.timor.flink.learning.a6processfunction;

import com.timor.flink.learning.dao.WaterSensor;
import com.timor.flink.learning.tools.UdfWaterSensorMap;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @Title: A2_TopN
 * @Package: com.timor.flink.learning.a6processfunction
 * @Description:
 * @Author: XXX
 * @Date: 2023/6/28 10:57
 * @Version:1.0
 */
public class A2_TopnWindowAll {
    public static void main(String[] args) throws Exception {

        //windowAll来做,优点是解决了topn全局问题，不会出现多个并行度多个topN
        // 缺点，只有一个并行度，效率低下

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        SingleOutputStreamOperator<WaterSensor> mapStrategy = env
                .socketTextStream("localhost", 7777)
                .map(new UdfWaterSensorMap())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs() * 1000L;
                                    }
                                })
                                .withIdleness(Duration.ofSeconds(1))

                );

       //用了windowal，就不需要keyBy了,老师代码里没有keyb，用了keyby之后还是多个并行度
       // KeyedStream<WaterSensor, String> keyBy = mapStrategy.keyBy(WaterSensor::getId);


        AllWindowedStream<WaterSensor, TimeWindow> windowAll = mapStrategy
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        //注意创建的map和list要放在process方法里面，因为每个窗口单独自己的
        //如果想共享map和lis，就放到process方法外面
        SingleOutputStreamOperator<String> process = windowAll.process(

                new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context
                            , Iterable<WaterSensor> elements, Collector<String> out) throws Exception {

                        HashMap<String, Integer> map = new HashMap<>();
                        ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();

                        for (WaterSensor element : elements) {

                            String id = element.getId();

                            if (map.containsKey(id)) {
                                map.put(id, map.get(id) + 1);
                            } else {
                                map.put(id, 1);
                            }

                        }

                        for (String key : map.keySet()) list.add(new Tuple2<>(key, map.get(key)));

                        //list排序
                        list.sort((a, b) -> {
                            if (a.f1 > b.f1) return 0;
                            else return 1;
                        });

                        System.out.println("list中元属有:"+list.size());
                        System.out.println( "list的hashcode是："+list.hashCode());


                        long start = context.window().getStart();
                        String format = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");

                        //预防list中数据数量不够2个i < Math.min(2,list.size()
                        for (int i = 0; i < Math.min(2, list.size()); i++) {
                            out.collect(list.get(i).toString() + "窗口开始时间为" + format);
                        }

                    }
                }
        );

        process.print();

        env.execute();


    }
}
