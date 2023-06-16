package com.timor.flink.learning.a5windows;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Title: A7_MyWaterMarkGenerator
 * @Package: com.timor.flink.learning.a5windows
 * @Description:
 * @Author: XXX
 * @Date: 2023/6/1 23:34
 * @Version:1.0
 */

//自定义规则，即模仿一个.forBoundedOutOfOrderness()返回值的类,即返回一个新的WatermarkStrategy
public class A8_MyPeriodWaterMarkGenerator {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socket = env.socketTextStream("localhost", 7777);

        //forGenerator方法来传入自定义生成watermark规则
        //问题 :forGenerator方法需要传入一个WatermarkGeneratorSupplier，但是老师模仿的实现的是WatermarkGenerator
        //这2个不一样WatermarkGenerator和WatermarkGeneratorSupplier没有父子关系为什么能传进去
       // WatermarkStrategy.forGenerator()

    }
}
