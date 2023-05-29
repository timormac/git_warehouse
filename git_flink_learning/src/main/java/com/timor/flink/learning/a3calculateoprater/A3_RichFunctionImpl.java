package com.timor.flink.learning.a3calculateoprater;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Title: A2_RichFunctionImpl
 * @Package: com.timor.flink.learning.a3calculateoprater
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/29 16:37
 * @Version:1.0
 */
    //open是并行度级别的，每个并行的线程在开启时会执行一只open，关闭时执行一次close，关闭指通过yarn cancel任务等方法

    //RichMapFunction实现了MapFunction还实现了AbstractRichFunction，
public class A3_RichFunctionImpl extends RichMapFunction<String, String > {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("localhost",7777);
        SingleOutputStreamOperator<String> map = ds.map( new A3_RichFunctionImpl() );
        map.print();
        env.execute();
    }

    @Override
    public String map(String value) throws Exception {
        return  "map:"+value;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println(
                "子任务编号=" + getRuntimeContext().getIndexOfThisSubtask()
                        + "，子任务名称=" + getRuntimeContext().getTaskNameWithSubtasks()
                        + ",调用open()");

    }

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println(
                "子任务编号=" + getRuntimeContext().getIndexOfThisSubtask()
                        + "，子任务名称=" + getRuntimeContext().getTaskNameWithSubtasks()
                        + ",调用close()");

    }


}
