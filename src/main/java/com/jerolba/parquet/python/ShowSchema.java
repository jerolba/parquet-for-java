package com.jerolba.parquet.python;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

public class ShowSchema {

    public static void main(String[] args) throws IOException {
        showSchema("nested_struct.parquet");
        showSchema("nested_collecton_struct.parquet");
        showSchema("nested_collecton_struct_compliant.parquet");
    }

    private static void showSchema(String fileName) throws IOException {
        System.out.println(fileName);
        Configuration conf = new Configuration();
        URL url = conf.getClass().getClassLoader().getResource(fileName);
        Path path = new Path(url.getFile());
        InputFile inputFile = HadoopInputFile.fromPath(path, conf);
        ParquetFileReader reader = ParquetFileReader.open(inputFile);
        MessageType schema = reader.getFileMetaData().getSchema();
        System.out.println(schema);

    }

}
