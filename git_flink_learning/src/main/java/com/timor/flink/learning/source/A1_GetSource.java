package com.timor.flink.learning.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Title: A1_CollectionSource
 * @Package: com.timor.flink.learning.source
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/26 18:01
 * @Version:1.0
 */
public class A1_GetSource {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从集合中创建DataStream
        DataStreamSource<Integer> collectionDS = env.fromCollection(Arrays.asList(1,2,3,4));

        //从文件中创建DataStream
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("input/word.txt")).build();
        DataStreamSource<String> fileDS = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file");

        //从socket获取
        DataStreamSource<String> stream = env.socketTextStream("project1", 7777);

        //从kafka获取
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder() //builder前面要指定范型方法
                .setBootstrapServers("project1:9092,project2:9092,project3:9092")
                .setGroupId("aaa")
                .setTopics("flink_word")
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
