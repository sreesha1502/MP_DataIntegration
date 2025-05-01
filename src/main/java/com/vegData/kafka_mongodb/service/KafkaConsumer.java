package com.vegData.kafka_mongodb.service;

import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertOneResult;
import com.vegData.kafka_mongodb.collection.Poles;

import org.springframework.messaging.handler.annotation.Header;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;

import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.scheduling.annotation.EnableAsync;
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

@Component
@Service
@Slf4j
@EnableAsync
@PropertySource(value = "classpath:application.properties")
public class KafkaConsumer {

    private MongoClient mongoClient;
    private MongoDatabase database;

    @Value("kafkaMsg")
    private String collectionName; // used @Value to read from app props file

    private final String imgDir = "/var/www/kafkaTest/";

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

    KafkaConsumer(@Value("${spring.data.mongodb.uri}") String mongoUri,
            @Value("${spring.data.mongodb.database}") String databaseName) {
        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();

        CodecRegistry pojoCodecRegistry = fromRegistries(getDefaultCodecRegistry(), fromProviders(pojoCodecProvider));

        this.mongoClient = MongoClients.create(mongoUri);
        this.database = mongoClient.getDatabase(databaseName).withCodecRegistry(pojoCodecRegistry);

    }

    @KafkaListener(topics = "${spring.kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
    public void consume(Poles data) {

        try {
            MongoCollection<Poles> collection = database.getCollection(collectionName, Poles.class);
            GeoJsonPoint location = new GeoJsonPoint(data.getGps().getCoordinates()[0],data.getGps().getCoordinates()[1]);
            
            System.out.println(location);
            data.setGps(null);
            data.setLocation(location);
            InsertOneResult result = collection.insertOne(data);

            if (result.wasAcknowledged()) {
                LOGGER.info("*** kafka Message saved **** ");
            } else {
                LOGGER.warn("*** Unable to save message from kafka **** ");
            }
        } catch (Exception e) {
            LOGGER.error("Error while consuming message", e.getCause());
        } finally {
            if (this.mongoClient != null) {
                this.mongoClient.close();
            }
        }
    }

    @KafkaListener(topics = "pole-images", groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactoryImage")
    public void ConsumeImage(byte[] image, @Header(KafkaHeaders.RECEIVED_KEY) String fileName) {
        try {
            Path filePath = Paths.get(imgDir + fileName);
            LOGGER.info("Image file path: " + filePath.toString());
            // Create the directory if it doesn't exist
            Files.createDirectories(filePath.getParent());
            Files.write(filePath, image);
            LOGGER.info("Image saved to: " + filePath.toString());
        } catch (Exception e) {
            LOGGER.error("Error while consuming image", e.getCause());
        }
    }
}
