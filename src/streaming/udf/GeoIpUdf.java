package com.aiwarehouse.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * GeoIP UDF：将 IP 地址映射到地区（巴西州）
 */
public class GeoIpUdf extends ScalarFunction {

    public String eval(String ip) {
        if (ip == null || ip.isEmpty()) return "UNKNOWN";
        // 实际实现需要集成 MaxMind GeoIP2 数据库
        // 这里返回模拟结果
        int lastOctet = Integer.parseInt(ip.split("\\.")[3]) % 27;
        String[] states = {"SP", "RJ", "MG", "BA", "PR", "RS", "PE", "CE", "PA", "MA",
                           "GO", "AM", "SC", "ES", "PB", "RN", "MT", "MS", "PI", "AL",
                           "SE", "RO", "TO", "AC", "AP", "RR", "DF"};
        return states[lastOctet];
    }
}
