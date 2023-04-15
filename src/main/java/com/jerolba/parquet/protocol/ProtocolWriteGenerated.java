package com.jerolba.parquet.protocol;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.proto.ProtoParquetWriter;

import com.jerolba.parquet.protocol.model.Trip;

public class ProtocolWriteGenerated {

    public static void main(String[] args) throws IOException {
        Path path = new Path("/tmp/commit2023-trips.parquet");
        OutputFile outputFile = HadoopOutputFile.fromPath(path, new Configuration());
        writeFile(outputFile);
    }

    public static void writeFile(OutputFile outputFile) throws IOException {
        try (ParquetWriter<Trip> writer = ProtoParquetWriter.<Trip>builder(outputFile)
                .withMessage(Trip.class)
                .withWriteMode(Mode.OVERWRITE)
                .build()) {
            var trip = Trip.newBuilder()
                    .setDispatchingBaseNum("A")
                    .setRequestDatetime(10000L)
                    .setTripMiles(1.3)
                    .setTripTime(130L)
                    .build();
            writer.write(trip);
        }
    }

}
