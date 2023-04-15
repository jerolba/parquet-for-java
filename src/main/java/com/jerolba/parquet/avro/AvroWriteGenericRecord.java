package com.jerolba.parquet.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;

public class AvroWriteGenericRecord {

    public static void main(String[] args) throws IOException {
        Path path = new Path("/tmp/commit2023-trips.parquet");
        OutputFile outputFile = HadoopOutputFile.fromPath(path, new Configuration());
        writeFile(outputFile);
    }

    public static void writeFile(OutputFile outputFile) throws IOException {
        Schema tripsSchema = SchemaBuilder.record("Trip")
                .fields()
                .optionalString("dispatching_base_num")
                .optionalLong("request_datetime")
                .requiredDouble("trip_miles")
                .optionalLong("trip_time")
                .endRecord();

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(tripsSchema)
                .withWriteMode(Mode.OVERWRITE)
                .build()) {
            GenericRecord trip = new GenericData.Record(tripsSchema);
            trip.put("dispatching_base_num", "A");
            trip.put("request_datetime", 10000L);
            trip.put("trip_miles", 1.3);
            trip.put("trip_time", 130L);
            writer.write(trip);
        }
    }

}
