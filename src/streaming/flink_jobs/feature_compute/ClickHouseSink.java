package com.aiwarehouse.flink.feature_compute;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * ClickHouse Sink（基于 JDBC，批量写入）
 */
public class ClickHouseSink extends RichSinkFunction<MinuteStats> {

    private final String tableName;
    private Connection conn;
    private PreparedStatement stmt;

    private static final String INSERT_SQL =
        "INSERT INTO %s (window_start, category, order_cnt, total_gmv, avg_price) VALUES (?, ?, ?, ?, ?)";

    public ClickHouseSink(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration params) throws Exception {
        String host = System.getenv().getOrDefault("CLICKHOUSE_HOST", "clickhouse");
        String user = System.getenv().getOrDefault("CLICKHOUSE_USER", "admin");
        String pass = System.getenv().getOrDefault("CLICKHOUSE_PASSWORD", "");
        conn = DriverManager.getConnection(
            "jdbc:clickhouse://" + host + ":8123/dws", user, pass);
        stmt = conn.prepareStatement(String.format(INSERT_SQL, tableName));
    }

    @Override
    public void invoke(MinuteStats value, Context ctx) throws Exception {
        stmt.setLong(1, value.getWindowEnd());
        stmt.setString(2, value.getCategory());
        stmt.setLong(3, value.getOrderCount());
        stmt.setDouble(4, value.getTotalGmv());
        stmt.setDouble(5, value.getAvgPrice());
        stmt.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (stmt != null) stmt.close();
        if (conn != null) conn.close();
    }
}
