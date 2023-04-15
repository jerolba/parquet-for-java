package com.jerolba.parquet.carpet;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;

import com.jerolba.carpet.CarpetReader;

public class CarpetRead {

    record Trip(String dispatching_base_num, long trip_time, double trip_miles, long request_datetime) {
    }

    public static void main(String[] args) throws IOException {
        for (int i = 0; i < 10; i++) {
            var content = readFile("/tmp/fhvhv_tripdata_2022-01.parquet");
            System.out.println(new Date() + ": " + content.size() + " records");
        }
    }

    public static List<Trip> readFile(String filePath) throws IOException {
        CarpetReader<Trip> reader = new CarpetReader<>(new File(filePath), Trip.class);
        List<Trip> result = reader.stream().toList();
        return result;
    }

}
