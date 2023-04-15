package com.jerolba.parquet.avro;

import org.apache.avro.Schema;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;

public class AvroReflection {

    public static <T> Schema generateSchema(Class<T> schemaClass) throws JsonMappingException {
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator();
        mapper.acceptJsonFormatVisitor(schemaClass, avroSchemaGenerator);
        AvroSchema schemaWrapper = avroSchemaGenerator.getGeneratedSchema();
        return new Schema.Parser().parse(schemaWrapper.getAvroSchema().toString());
    }

    public static class Trip {

        private String dispatching_base_num;
        private Double trip_miles;
        private Long trip_time;
        private Long request_datetime;

        public Trip() {
        }

        public Trip(String dispatching_base_num, Long request_datetime, Double trip_miles, Long trip_time) {
            this.dispatching_base_num = dispatching_base_num;
            this.request_datetime = request_datetime;
            this.trip_time = trip_time;
            this.trip_miles = trip_miles;
        }

        public String getDispatching_base_num() {
            return dispatching_base_num;
        }

        public Double getTrip_miles() {
            return trip_miles;
        }

        public Long getTrip_time() {
            return trip_time;
        }

        public Long getRequest_datetime() {
            return request_datetime;
        }

        public void setDispatching_base_num(String dispatching_base_num) {
            this.dispatching_base_num = dispatching_base_num;
        }

        public void setTrip_miles(Double trip_miles) {
            this.trip_miles = trip_miles;
        }

        public void setTrip_time(Long trip_time) {
            this.trip_time = trip_time;
        }

        public void setRequest_datetime(Long request_datetime) {
            this.request_datetime = request_datetime;
        }

    }

}
