package com.timor.flink.learning.a2source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Title: A3_DataGeneratorSource
 * @Package: com.timor.flink.learning.a2source
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/30 15:04
 * @Version:1.0
 */
public class A3_DataGeneratorSource {

    public static void main(String[] args) throws Exception {

        //数据生成器source 一般用于压测，可以控制数据生成速度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * DataGeneratorSource，四个参数：
         *     第一个： GeneratorFunction接口，需要实现， 重写map方法， 输入类型固定是Long
         *     第二个： long类型， 自动生成的数字序列（从0自增）的最大值(小于)，达到这个值就停止了
         *     第三个： 限速策略， 比如 每秒生成几条数据
         *     第四个： 返回的类型
         */
        // 数据生成器DataGeneratorSource<String>的范型和GeneratorFunction接口输出类型相同
        DataGeneratorSource dataGeneratorSource = new DataGeneratorSource<String>(
                (Long value)-> "Number"+value, //GeneratorFunction<Long, OUT>，参数中第一个范型为Long已经写死
                Long.MAX_VALUE,    //从0自增，到1000就停止，如果想弄成无界流就用Max
                RateLimiterStrategy.perSecond(200), //每秒200条数据
                Types.STRING  //这个是Flink
        );

        DataStreamSource dataStreamSource = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        dataStreamSource.print();
        env.execute();

    }
}
