package com.jerolba.parquet.duckdb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DuckDbRead {

    record Trip(String baseNum, long tripTime, double tripMiles, long requestDatetime) {
    }

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 10; i++) {
            var content = readFile("/tmp/fhvhv_tripdata_2022-01.parquet");
            System.out.println(new Date() + ": " + content.size() + " records");
        }
    }

    public static List<Trip> readFile(String filePath) throws IOException, ClassNotFoundException, SQLException {
        Class.forName("org.duckdb.DuckDBDriver");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        String sql = "select dispatching_base_num, trip_time, trip_miles, request_datetime from '" + filePath + "'";
        var ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        List<Trip> result = new ArrayList<>();
        while (rs.next()) {
            String base = rs.getString("dispatching_base_num");
            long tripTime = rs.getLong("trip_time");
            double tripMiles = rs.getDouble("trip_miles");
            long requestTime = rs.getLong("request_datetime");
            result.add(new Trip(base, tripTime, tripMiles, requestTime));
        }
        return result;
    }

}
