package org.globex.retail.streams.serde;

import java.util.Comparator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;
import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.globex.retail.streams.collectors.FixedSizePriorityQueue;
import org.globex.retail.streams.model.ProductScore;

public class FixedSizePriorityQueueSerde implements Serde<FixedSizePriorityQueue> {

    Comparator<ProductScore> comparator;

    int maxSize;

    ObjectMapper objectMapper;

    public FixedSizePriorityQueueSerde(Comparator<ProductScore> comparator, int maxSize) {
        FixedSizePriorityQueueSerializer serializer = new FixedSizePriorityQueueSerializer();
        FixedSizePriorityQueueDeserializer deserializer = new FixedSizePriorityQueueDeserializer(comparator, maxSize);
        objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(FixedSizePriorityQueue.class, deserializer);
        module.addSerializer(FixedSizePriorityQueue.class, serializer);
        objectMapper.registerModule(module);
    }

    @Override
    public void close() {
        Serde.super.close();
    }

    @Override
    public Serializer<FixedSizePriorityQueue> serializer() {
        return new ObjectMapperSerializer<>(objectMapper);
    }

    @Override
    public Deserializer<FixedSizePriorityQueue> deserializer() {
        return new ObjectMapperDeserializer<>(FixedSizePriorityQueue.class, objectMapper);
    }
}
