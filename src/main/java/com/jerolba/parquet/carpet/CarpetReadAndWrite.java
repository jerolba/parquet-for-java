package com.jerolba.parquet.carpet;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.CarpetWriter;

public class CarpetReadAndWrite {

    record Trip(String dispatching_base_num, long trip_time, double trip_miles, long request_datetime) {
    }

    public static void main(String[] args) throws IOException {
        var content = readFile("/tmp/fhvhv_tripdata_2022-01.parquet");
        writeFile("/tmp/fhvhv_tripdata_2022-01-copy.parquet", content);
    }

    public static List<Trip> readFile(String filePath) throws IOException {
        CarpetReader<Trip> reader = new CarpetReader<>(new File(filePath), Trip.class);
        List<Trip> result = reader.stream().toList();
        return result;
    }

    public static void writeFile(String filePath, List<Trip> data) throws IOException {
        FileOutputStream fos = new FileOutputStream(filePath);
        try (CarpetWriter<Trip> writer = new CarpetWriter.Builder<>(fos, Trip.class)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            writer.write(data);
        }
    }

}
