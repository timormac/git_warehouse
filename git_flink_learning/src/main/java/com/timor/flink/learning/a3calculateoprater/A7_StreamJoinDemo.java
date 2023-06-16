package com.timor.flink.learning.a3calculateoprater;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Title: A7_StreamJoinDeomo
 * @Package: com.timor.flink.learning.a3calculateoprater
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/30 10:33
 * @Version:1.0
 */
public class A7_StreamJoinDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //等并行度为1时，不keyby没问题，当多并行度时必须keyby
        env.setParallelism(3);

        DataStreamSource<Tuple2<String, Integer>> s1 = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 2),
                Tuple2.of("b", 3),
                Tuple2.of("c", 4)
        );

        DataStreamSource<Tuple2<String, String>> s2 = env.fromElements(
                Tuple2.of("a", "a1"),
                Tuple2.of("a", "a2"),
                Tuple2.of("b", "b1"),
                Tuple2.of("c", "b1")
        );

        ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, String>> connect = s1.connect(s2);

        //  必须keyby才能保证2流能关联到,connectstream的keyby和DataStreamSource的keyby不同
        //  传2个匿名方法，keyBy 有多个方法重，用2个keySelector的方法的参数方法
        ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, String>> keyby = connect.keyBy(
            tuple->tuple.f0, tuple->tuple.f0
        );

        //2流join思路:数据是一条条过来的，有先后顺序。来一条数据先去存起来，然后去关联另一个流
        SingleOutputStreamOperator<Tuple2<String, String>> process = keyby.process(
                new CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, String>, Tuple2<String, String>>() {

                    //声明在这里的变量，才能在2个方法中同时访问
                     HashMap<String, List<Integer>> s1cache = new HashMap<String, List<Integer>>();
                     HashMap<String, List<String>> s2cache = new HashMap<String, List<String>>();

                    //
                    @Override
                    public void processElement1(Tuple2<String, Integer> value, CoProcessFunction<Tuple2<String, Integer>,
                            Tuple2<String, String>, Tuple2<String, String>>.Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
                        //放入元属,相同id有多条数据所以用list存values
                        if (!s1cache.containsKey(value.f0)) {
                            List<Integer> list = new ArrayList<>();
                            list.add(value.f1);
                            s1cache.put(value.f0, list);

                        } else {
                            List<Integer> list1 = s1cache.get(value.f0);
                            list1.add(value.f1);
                            s1cache.put(value.f0, list1);
                        }

                        //关联s2
                        if (s2cache.containsKey(value.f0)) {
                            for (String s : s2cache.get(value.f0)) {
                                String str = "s1执行--s1:"+value.f1+","+ "s2:"+s ;
                                out.collect(Tuple2.of(value.f0,str ));
                            }

                        }

                        //查看当前cache数据
                        String listStr = "s1集合当前：";
                        for (Map.Entry<String, List<Integer>> s : s1cache.entrySet()) {
                            listStr = listStr + s.getKey() + ":";
                            for (Integer integer : s.getValue()) {
                                listStr =listStr + integer +",";
                            }

                        }
                        //System.out.println(listStr);

                    }

                    @Override
                    public void processElement2(Tuple2<String, String> value, CoProcessFunction<Tuple2<String, Integer>,
                            Tuple2<String, String>, Tuple2<String, String>>.Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
                        //放入元属,相同id有多条数据所以用list存values
                        if (!s2cache.containsKey(value.f0)) {
                            List<String> list = new ArrayList<>();
                            list.add(value.f1);
                            s2cache.put(value.f0, list);

                        } else {
                            List<String> list1 = s2cache.get(value.f0);
                            list1.add(value.f1);
                            s2cache.put(value.f0, list1);
                        }

                        //关联s1
                        if (s1cache.containsKey(value.f0)) {
                            for (Integer s : s1cache.get(value.f0)) {
                                String str = "s2执行关联--s2:"+value.f1+"s1:"+s ;
                                out.collect(  Tuple2.of(value.f0,str)   );
                            }


                        }

                        //查看当前cache数据
                        String listStr = "s2集合当前:";
                        for (Map.Entry<String, List<String>> s : s2cache.entrySet()) {
                            listStr = listStr + s.getKey() +"：";
                            for (String integer : s.getValue()) {
                                listStr =listStr  + integer +",";
                            }

                        }
                        //System.out.println(listStr);
                    }
                });

        process.print();

        env.execute();




    }


}
