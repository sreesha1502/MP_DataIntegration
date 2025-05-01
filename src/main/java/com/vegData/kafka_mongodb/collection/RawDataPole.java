package com.vegData.kafka_mongodb.collection;

import com.fasterxml.jackson.annotation.JsonInclude;

import lombok.Data;

@Data
@JsonInclude
public class RawDataPole {
    private NmeaInfo nmeaInfo;
    private CameraInfo cameraInfo;
}
