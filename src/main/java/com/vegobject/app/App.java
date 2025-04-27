package com.vegobject.app;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class App {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello World!");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "dt14.idi.ntnu.no:9092");
        properties.setProperty("group.id", "vegobject");

        KafkaSource<String> consumer = KafkaSource.<String>builder()
                .setProperties(properties)
                .setTopics("poles-sync")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        env.fromSource(consumer, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Kafka Source").map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println(value);
                return value;
            }
        }).print();
        env.execute("Kafka Flink MongoDB Example");
    }
    
}
