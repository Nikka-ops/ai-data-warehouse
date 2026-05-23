package com.aiwarehouse.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * User-Agent 解析 UDF
 */
public class DeviceParserUdf extends ScalarFunction {

    public String eval(String userAgent) {
        if (userAgent == null) return "UNKNOWN";
        String ua = userAgent.toLowerCase();
        if (ua.contains("mobile") || ua.contains("android") || ua.contains("iphone")) {
            return "MOBILE";
        } else if (ua.contains("tablet") || ua.contains("ipad")) {
            return "TABLET";
        }
        return "DESKTOP";
    }
}
