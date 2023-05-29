package com.timor.flink.learning.a1wordcountdemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Title: A1_WordCount
 * @Package: com.cbk.demo
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/24 09:20
 * @Version:1.0
 */
public class A1_WordCountBatch {

    public static void main(String[] args) throws Exception {

        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> lineDS = env.readTextFile("input_data/wordcount.txt");
        TimoFlatMap timoFM = new TimoFlatMap();

        //flatMap方法需要传一个对象，是实现了FlatMapFunction接口的对象
        FlatMapOperator<String, Tuple2<String, Integer>> operator = lineDS.flatMap(timoFM);
        AggregateOperator<Tuple2<String, Integer>> sum = operator.groupBy(0).sum(1);
        sum.print();


        //这个是匿名内部类的方式
/*       lineDS.flatMap(

                new FlatMapFunction<String, Tuple2<String,Integer>>() {

                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

                        String[] arr = s.split(" ");

                        for (String s1 : arr) {

                            collector.collect( new Tuple2<>(s1,1) );

                        }
                    }
                }
        );*/

    }


    public static class TimoFlatMap implements FlatMapFunction<String,Tuple2<String,Integer> > {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

            String[] arr = s.split(" ");

            for (String s1 : arr) {

                collector.collect( new Tuple2<>(s1,1) );

            }

        }
    }

}
