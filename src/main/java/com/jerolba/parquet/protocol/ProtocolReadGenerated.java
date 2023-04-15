package com.jerolba.parquet.protocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.proto.ProtoParquetReader;
import org.apache.parquet.proto.ProtoReadSupport;

import com.jerolba.parquet.protocol.model.Trip;

public class ProtocolReadGenerated {

    public static void main(String[] args) throws IOException {
        Path path = new Path("/tmp/fhvhv_tripdata_2022-01.parquet");
        InputFile inputFile = HadoopInputFile.fromPath(path, new Configuration());
        var content = readFile(inputFile);
        System.out.println(content.size() + " records");
    }

    public static List<Trip> readFile(InputFile inputFile) throws IOException {
        List<Trip> result = new ArrayList<>();
        Configuration conf = new Configuration();
        ProtoReadSupport.setProtobufClass(conf, Trip.class.getName());
        String projection = """
                message schema {
                  optional binary dispatching_base_num (STRING);
                  optional int64 request_datetime (TIMESTAMP(MICROS,false));
                  optional double trip_miles;
                  optional int64 trip_time;
                }
                """;
        ProtoReadSupport.setRequestedProjection(conf, projection);
        try (var reader = ProtoParquetReader.<Trip.Builder>builder(inputFile)
                .withConf(conf)
                .build()) {
            Trip.Builder tripBuilder = null;
            while ((tripBuilder = reader.read()) != null) {
                result.add(tripBuilder.build());
            }
        }
        return result;
    }

}
