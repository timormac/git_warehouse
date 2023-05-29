package com.timor.flink.learning.a2source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Title: A2_KafkaSourceDemo
 * @Package: com.timor.flink.learning.source
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/29 09:32
 * @Version:1.0
 */
public class A2_KafkaSourceDemo {

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

        //下面这个是kafka的auto.reset.offsets
        //earlist:如果有offset,从offset继续消费，没有从最早消费
        //lastest:如果有offset,从offset继续消费，没有从最新消费

        //flink的offset和kafka的不同，earlist一定从最早，latest一定从最新，不管有没有offset

        DataStreamSource<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource");

        ds.print();

        env.execute();
    }
}
