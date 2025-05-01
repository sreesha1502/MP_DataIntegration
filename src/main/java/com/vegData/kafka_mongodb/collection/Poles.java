package com.vegData.kafka_mongodb.collection;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import com.fasterxml.jackson.annotation.JsonInclude;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Document(collection = "poles")
@JsonInclude(JsonInclude.Include.NON_NULL)
@AllArgsConstructor
@NoArgsConstructor
public class Poles {

    @Id
    private String id;
    @Field("id")
    private String poleId;
    private double altitude;
    private int speed;
    private int fixType;
    private double courseOverGround;
    private double hdop;
    private String capturedDate;
    private Geometry gps;
    private GeoJsonPoint location;

}
