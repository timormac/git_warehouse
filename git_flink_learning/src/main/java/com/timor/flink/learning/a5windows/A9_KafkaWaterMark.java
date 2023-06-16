package com.timor.flink.learning.a5windows;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @Title: A9_KafkaWaterMark
 * @Package: com.timor.flink.learning.a5windows
 * @Description:
 * @Author: XXX
 * @Date: 2023/6/2 09:47
 * @Version:1.0
 */
public class A9_KafkaWaterMark {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从kafka获取
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder() //builder前面要指定范型方法
                .setBootstrapServers("project1:9092,project2:9092,project3:9092")
                .setGroupId("aaa")
                .setTopics("flink_word_topic")
                .setValueOnlyDeserializer(new SimpleStringSchema())//指定从kafka获取的数据的反序列化器
                .setStartingOffsets(OffsetsInitializer.latest())  //设置消费起始offset
                .build();

        // 这里的forBoundedOutOfOrderness，不需要设置时间时间提取，ds默认会调用kafka消息的ts作为watermark,也可以手动指定
        DataStreamSource<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2)), "kafkasource");

        ds.print();

        env.execute();


    }

}
