package com.jerolba.parquet.carpet;

import java.io.FileOutputStream;
import java.io.IOException;

import com.jerolba.carpet.CarpetWriter;

public class CarpetWrite {

    record Trip(String dispatching_base_num, long trip_time, double trip_miles, long request_datetime) {
    }

    public static void main(String[] args) throws IOException {
        writeFile("/tmp/commit2023-trips.parquet");
    }

    public static void writeFile(String filePath) throws IOException {
        FileOutputStream fos = new FileOutputStream(filePath);
        try (CarpetWriter<Trip> writer = new CarpetWriter<>(fos, Trip.class)) {
            writer.write(new Trip("A", 10000L, 1.3, 130L));
            writer.write(new Trip("B", 20000L, 1.2, 180L));
        }
    }

}
