package com.jerolba.parquet.avro;

import static com.jerolba.parquet.avro.AvroReflection.generateSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

import com.jerolba.parquet.avro.AvroReflection.Trip;

public class AvroReadReflection {

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
        Configuration conf = new Configuration();
        Schema schema = generateSchema(Trip.class);
        AvroReadSupport.setRequestedProjection(conf, schema);

        try (ParquetReader<Trip> reader = AvroParquetReader.<Trip>builder(inputFile)
                .withDataModel(ReflectData.get())
                .withConf(conf)
                .build()) {
            Trip trip = null;
            while ((trip = reader.read()) != null) {
                result.add(trip);
            }
        }
        return result;
    }

}
