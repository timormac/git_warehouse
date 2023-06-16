package com.timor.flink.learning.a5windows;

import com.timor.flink.learning.dao.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
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
 * @Title: A3_ProcessWindowFunciton
 * @Package: com.timor.flink.learning.a5windows
 * @Description:
 * @Author: XXX
 * @Date: 2023/6/1 12:34
 * @Version:1.0
 */
public class A3_ProcessWindowFuncitonDemo {

    public static void main(String[] args) throws Exception {

        //ProcessWindowFunciton全窗口函数，是必须数据全来了之后，先缓存起来，然后等10s窗口结束统一计算
        //aggregate是来一条算一条，多个数据进来只返回一条数据。
        //ProcessWindowFunciton数据来了先不算，先缓存，等到窗口时间到了，把10s内的数据统一计算
        //Process可以获取到上下文
        //两者结合使用
        //还是没想明白区别

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

        // ProcessWindowFunction的范型<IN, OUT, KEY, W>,用的是wiondow调用的process方法，看window的范型是<WaterSensor, String, TimeWindow>
        //所以IN = WaterSensor , OUT=自己想要什么 , KEY=String, W=TimeWindow

        //aggregate方法，一个窗口之后输出的数据只有一个
        // process方法是只调用一次重写process函，out.collect代码执行几次，那么返回几个元属
        SingleOutputStreamOperator<String> process = window.process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context,
                                        Iterable<WaterSensor> elements, Collector<String> out) throws Exception {

                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        String startfm = DateFormatUtils.format(start, "HH:mm:ss.SSS");
                        String endfm = DateFormatUtils.format(end, "HH:mm:ss.SSS");
                        System.out.println("调用了process方法");

                        out.collect("起始时间:" + startfm + "--" + "终止时间" + endfm +":");

                        for (WaterSensor element : elements) {
                            out.collect( "collect收集到的"+ element.toString());
                        }

                    }
                }
        );
        process.print();
        env.execute();


    }



}
