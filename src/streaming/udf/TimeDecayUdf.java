package com.aiwarehouse.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * 时间衰减 UDF：对历史事件施加指数衰减权重
 */
public class TimeDecayUdf extends ScalarFunction {

    /** @param eventTimeMs 事件时间（毫秒）
     *  @param halfLifeHours 半衰期（小时）
     *  @return 衰减权重 [0, 1] */
    public double eval(long eventTimeMs, double halfLifeHours) {
        long nowMs = System.currentTimeMillis();
        double hoursElapsed = (nowMs - eventTimeMs) / 3_600_000.0;
        return Math.pow(0.5, hoursElapsed / halfLifeHours);
    }
}
