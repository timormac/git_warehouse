package com.timor.flink.learning.a3calculateoprater;

import com.timor.flink.learning.dao.WaterSensor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Title: A4_SteamSplitUnion
 * @Package: com.timor.flink.learning.a3calculateoprater
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/29 17:04
 * @Version:1.0
 */
public class A4_SteamSplitUnion {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> ds = env.fromElements("s1", "s2", "s3");
        SingleOutputStreamOperator<WaterSensor> map = ds.map((str) -> new WaterSensor(str, 1L, 2));

        //定义流的标签时要放在算子外面，这样算子内所有的能调用
        OutputTag s1 = new OutputTag("s1stream", Types.POJO(WaterSensor.class));
        OutputTag s2 = new OutputTag("s2stream", Types.POJO(WaterSensor.class));

        //分流侧输出流,返回的是主流
        SingleOutputStreamOperator<WaterSensor> process = map.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                if ("s1".equals(value.id)) {
                    //如果是s1放到s1侧输出流中,s2放在s2侧流，其他的放在主流
                    //通过ctx上下文来创建侧输出流
                    ctx.output(s1, value);
                } else if ("s2".equals(value.id)) {
                    ctx.output(s2, value);
                } else {
                    //放到主流中
                    out.collect(value);
                }
            }
        });

        SideOutputDataStream sideOutputs1 = process.getSideOutput(s1);
        SideOutputDataStream sideOutputs2 = process.getSideOutput(s2);

        //这个是主流,print(说明),在打印时有个标识
        process.print("主流");
        sideOutputs1.print("s1");
        sideOutputs2.print("s2");

        env.execute();


    }


}
