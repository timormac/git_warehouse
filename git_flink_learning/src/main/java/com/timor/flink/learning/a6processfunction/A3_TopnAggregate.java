package com.timor.flink.learning.a6processfunction;

import com.timor.flink.learning.dao.WaterSensor;
import com.timor.flink.learning.tools.UdfWaterSensorMap;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @Title: A3_TopnAggregate
 * @Package: com.timor.flink.learning.a6processfunction
 * @Description:
 * @Author: XXX
 * @Date: 2023/6/28 14:56
 * @Version:1.0
 */
public class A3_TopnAggregate {

    public static void main(String[] args) throws Exception {

        /**
         * TODO 思路二： 使用 KeyedProcessFunction实现
         * 1、按照vc做keyby，开窗，分别count
         *    ==》 增量聚合，计算 count
         *    ==》 全窗口，对计算结果 count值封装 ，  带上 窗口结束时间的 标签
         *          ==》 为了让同一个窗口时间范围的计算结果到一起去
         *
         * 2、对同一个窗口范围的count值进行处理： 排序、取前N个
         *    =》 按照 windowEnd做keyby
         *    =》 使用process， 来一条调用一次，需要先存，分开存，用HashMap,key=windowEnd,value=List
         *      =》 使用定时器，对 存起来的结果 进行 排序、取前N个
         */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        SingleOutputStreamOperator<WaterSensor> mapStrategy = env
                .socketTextStream("localhost", 7777)
                .map(new UdfWaterSensorMap())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs() * 1000L;
                                    }
                                })
                                .withIdleness(Duration.ofSeconds(1))

                );



        KeyedStream<WaterSensor, String> keyBy = mapStrategy.keyBy(sensor -> sensor.getId());

        WindowedStream<WaterSensor, String, TimeWindow> window = keyBy.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<Tuple4<String, Integer, String, Long>> aggregate = window.aggregate(new TopnAggFunc(), new TopnProcessWindowFunc());

        KeyedStream<Tuple4<String, Integer, String, Long>, Long> keyBy1 = aggregate.keyBy(t -> t.f3);

        SingleOutputStreamOperator<String> process = keyBy1.process(new TopnKeyedProcessFunc());

        process.print();


        env.execute();



    }

    //这里只按key去记录出现次数，不传递key，在process方法中能获取到key
   static class TopnAggFunc  implements AggregateFunction<WaterSensor,Integer, Integer>{

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            int  num = accumulator+1;
            System.out.println("调用一次add方法，key为:"+value.getId()+"当前key出现次数"+ num );
            return accumulator+1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            System.out.println("调用getResult方法");
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    //第一个范型是In的类型，与Aggregate结合就是Aggregate的输出类型，第二范型是输出类型
    //第三个范型是keyBy时的key的类型，第4个范型是窗口类型
   static  class TopnProcessWindowFunc extends ProcessWindowFunction<Integer, Tuple4<String, Integer,String,Long>, String, TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<Integer, Tuple4<String, Integer,String,Long>, String, TimeWindow>.Context context
                , Iterable<Integer> elements, Collector< Tuple4<String, Integer,String,Long>> out) throws Exception {

            System.out.println("调用process方法");

            long end = context.window().getEnd();
            String endDate = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");

            //agg传递过来的数据只有一条，迭代器中数据只有一条
            for (Integer element : elements) {
                System.out.println("process中iterble中数据"+elements
                );
            }

            Integer num = elements.iterator().next();

            out.collect( new Tuple4<>(s,num,endDate,end) );

        }
    }

    //有个问题就是为什么,不同命名都是按key10000分组的，但是map的hashcode不同，并且最后Ontimer的获取的是c的hashcode
   //hashcode应该不是地址值，当集合中添加元素时,hashcode会发生变化


/*
    processElement方法调用一次：(a,3,1970-01-01 08:00:10.000,10000)
    10000map的hashcode是:-1885273959
    ctx的timestamp为9999
    processElement方法调用一次：(b,2,1970-01-01 08:00:10.000,10000)
    10000map的hashcode是:-198883697
    ctx的timestamp为9999
    processElement方法调用一次：(c,1,1970-01-01 08:00:10.000,10000)
    10000map的hashcode是:539557367
    ctx的timestamp为9999
    调用定时器
    OnTimer方法中调用map的HashCode539557367
    获取的ctx.getCurrentKey()为:10000
    2> 窗口为10000前2个top为:a出现3次b出现2次
* */
   static class TopnKeyedProcessFunc  extends KeyedProcessFunction< Long,Tuple4<String, Integer, String,Long>,String >{

       HashMap<Long, ArrayList< Tuple4<String, Integer, String,Long>> >  map = new  HashMap<>();

       @Override
       public void processElement(Tuple4<String, Integer, String, Long> value, KeyedProcessFunction<Long,
               Tuple4<String, Integer, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {

           System.out.println( "processElement方法调用一次："+value.toString() );

           if( map.containsKey(value.f3) )   map.get(value.f3).add(value);
           else {
               ArrayList< Tuple4<String, Integer, String, Long>>  tempList = new ArrayList<>();
               tempList.add(value);
               map.put(value.f3, tempList);
           }

           System.out.println(  value.f3+ "map的hashcode是:"+ map.hashCode());

           //因为相同key的数据是一条条来的，所以不确定什么时候相同key的数据收集完整，所以需要设置定时器
           //因为是按事件时间，并且waterMark的推进是通过数据来重新获取的，不同算子之间的waterMark没有联系
           //当设置窗口结束时间+1ms时，当watermark触发定时器时，那么之前的所有数据就来齐了。
           TimerService timerService = ctx.timerService();
           timerService.registerEventTimeTimer( value.f3 + 10L);

           System.out.println(  "ctx的timestamp为" + ctx.timestamp());

       }

       @Override
       public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple4<String, Integer, String, Long>, String>.OnTimerContext ctx,
                           Collector<String> out) throws Exception {

           super.onTimer(timestamp, ctx, out);
           System.out.println("调用定时器");

           System.out.println( "OnTimer方法中调用map的HashCode"+ map.hashCode() );


           Long key = ctx.getCurrentKey();
           System.out.println("获取的ctx.getCurrentKey()为:"+key);
           ArrayList<Tuple4<String, Integer, String, Long>> list = map.get(key);
           list.sort( (a,b)->{
               if(a.f1>b.f1) return 0;
               else return 1;
           } );

           String result = "窗口为"+key+"前2个top为:";

           for (int i = 0; i < Math.min(  list.size(),2  ) ; i++) {
                 result =  result + list.get(i).f0 + "出现"+ list.get(i).f1+"次";
           }

           out.collect(result );

       }
   }


}
