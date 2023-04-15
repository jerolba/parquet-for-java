package com.jerolba.parquet.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

public class AvroReadGenericRecord {

    record Trip(String origin, double miles, long time, long request) {

    }

    public static void main(String[] args) throws IOException {
        Path path = new Path("/tmp/fhvhv_tripdata_2022-01.parquet");
        InputFile inputFile = HadoopInputFile.fromPath(path, new Configuration());
        for (int i = 0; i < 10; i++) {
            var content = readFile(inputFile);
            System.out.println(new Date() + ": " + content.size() + " records");
        }
    }

    public static List<Trip> readFile(InputFile inputFile) throws IOException {
        List<Trip> result = new ArrayList<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
            GenericRecord record = null;
            while ((record = reader.read()) != null) {
                Utf8 origin = (Utf8) record.get("dispatching_base_num");
                Double miles = (Double) record.get("trip_miles");
                Long time = (Long) record.get("trip_time");
                Long request = (Long) record.get("request_datetime");
                result.add(new Trip(origin.toString(), miles, time, request));
            }
        }
        return result;
    }

}
