package com.timor.flink.learning.a5windows;

import com.timor.flink.learning.dao.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Title: B1_WindowDelayDataP
 * @Package: com.timor.flink.learning.a5windows
 * @Description:
 * @Author: XXX
 * @Date: 2023/6/12 14:42
 * @Version:1.0
 */
public class B1_WindowDelayData {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<WaterSensor> map = ds.map(s -> {
            String[] split = s.split(",");
            return new WaterSensor(split[0], Long.valueOf(split[1]), 123);
        });

        WatermarkStrategy<WaterSensor> strategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2)) //水位线延迟2s更新
                .withIdleness(Duration.ofSeconds(6)) //空闲等待时间6s,到6s自动更新水位线
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        System.out.println("调用了获取时间时间方法:" + element.toString() + "recordTS:" + recordTimestamp);
                        return element.getTs() * 1000L;
                    }
                });

        SingleOutputStreamOperator<WaterSensor> doneStrategy = map.assignTimestampsAndWatermarks(strategy);

        KeyedStream<WaterSensor, String> keyBy = doneStrategy.keyBy(sensor -> sensor.getId());

        OutputTag<WaterSensor> lateTag = new OutputTag<>("late-data", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<String> process = keyBy
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) //窗口大小是10s
                .allowedLateness(Time.seconds(2)) //窗口延迟2S关闭
                .sideOutputLateData(lateTag) // 关窗后的迟到数据，放入侧输出流
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context,
                                        Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        System.out.println("调用了process方法一次");
                        for (WaterSensor element : elements) {
                            System.out.println("正在遍历迭代器");
                            out.collect(element.toString() + "||");
                        }
                    }
                });

        process.print();

        SideOutputDataStream<WaterSensor> sideOutput = process.getSideOutput(lateTag);
        sideOutput.printToErr("关窗后的迟到数据");
        env.execute();

    }

}
