package com.aiwarehouse.flink.feature_compute;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 1 分钟窗口聚合：计算订单数、GMV、平均客单价
 */
public class WindowAggregator
        implements AggregateFunction<OrderEvent, MinuteStats.Accumulator, MinuteStats> {

    @Override
    public MinuteStats.Accumulator createAccumulator() {
        return new MinuteStats.Accumulator();
    }

    @Override
    public MinuteStats.Accumulator add(OrderEvent value, MinuteStats.Accumulator acc) {
        acc.orderCount++;
        acc.totalGmv += value.getPrice() * value.getQuantity();
        acc.totalPrice += value.getPrice();
        acc.category = value.getCategory();
        return acc;
    }

    @Override
    public MinuteStats getResult(MinuteStats.Accumulator acc) {
        MinuteStats result = new MinuteStats();
        result.setOrderCount(acc.orderCount);
        result.setTotalGmv(acc.totalGmv);
        result.setAvgPrice(acc.orderCount > 0 ? acc.totalPrice / acc.orderCount : 0.0);
        result.setCategory(acc.category);
        result.setWindowEnd(System.currentTimeMillis());
        return result;
    }

    @Override
    public MinuteStats.Accumulator merge(MinuteStats.Accumulator a, MinuteStats.Accumulator b) {
        a.orderCount += b.orderCount;
        a.totalGmv += b.totalGmv;
        a.totalPrice += b.totalPrice;
        return a;
    }
}
