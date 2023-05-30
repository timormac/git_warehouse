package com.timor.flink.learning.dao;

import java.util.Objects;

/**
 * @Title: WaterSensor
 * @Package: com.timor.flink.learning.dao
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/29 09:15
 * @Version:1.0
 */

/*Flink对POJO类型的要求如下：

        l 类是公有（public）的
        l 有一个无参的构造方法
        l 所有属性都是公有（public）的
        l 所有属性的类型都是可以序列化的
        并且实现了searilized接口*/
public class WaterSensor {

    public   String id ;
    public  Long  ts;
    public  Integer vc;

    public WaterSensor() {

    }

    public WaterSensor(String id, Long ts, Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTs() {
        return ts;
    }

    public Integer getVc() {
        return vc;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WaterSensor that = (WaterSensor) o;
        return Objects.equals(id, that.id) && Objects.equals(ts, that.ts) && Objects.equals(vc, that.vc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, ts, vc);
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }
}
