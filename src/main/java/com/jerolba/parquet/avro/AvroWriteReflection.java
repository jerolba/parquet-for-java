package com.jerolba.parquet.avro;

import java.io.IOException;

import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;

import com.jerolba.parquet.avro.AvroReflection.Trip;

public class AvroWriteReflection {

    public static void main(String[] args) throws IOException {
        Path path = new Path("/tmp/commit2023-trips.parquet");
        OutputFile outputFile = HadoopOutputFile.fromPath(path, new Configuration());
        writeFile(outputFile);
    }

    public static void writeFile(OutputFile outputFile) throws IOException {
        try (ParquetWriter<Trip> writer = AvroParquetWriter.<Trip>builder(outputFile)
                .withDataModel(ReflectData.get())
                .withSchema(AvroReflection.generateSchema(Trip.class))
                .withWriteMode(Mode.OVERWRITE)
                .build()) {
            writer.write(new Trip("A", 10000L, 1.3, 130L));
            writer.write(new Trip("B", 20000L, 1.2, 180L));
        }
    }

}
