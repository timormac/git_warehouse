package com.timor.flink.learning.a5windows;

/**
 * @Title: A7_BreakPointWaterMarkGenerator
 * @Package: com.timor.flink.learning.a5windows
 * @Description:
 * @Author: XXX
 * @Date: 2023/6/2 09:40
 * @Version:1.0
 */
public class A7_BreakPointWaterMarkGenerator {

    public static void main(String[] args) {

        //默认是周期性调用方法每0.2s调用一次onPeriodicEmit， 会查看一下onEvent最大waterMark值，并将watermark推进
        //当数据很多时,用这种周期式比较好
        //断点式是每个事件都会去推进一次watermark，数据少时，这种时间触发断点式比较好


    }

}
