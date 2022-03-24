package org.globex.retail.streams.serde;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.globex.retail.streams.collectors.FixedSizePriorityQueue;
import org.globex.retail.streams.model.ProductScore;

public class FixedSizePriorityQueueDeserializer extends StdDeserializer<FixedSizePriorityQueue<ProductScore>> {

    private Comparator<ProductScore> comparator;

    int maxSize;

    public FixedSizePriorityQueueDeserializer(Comparator<ProductScore> comparator, int maxSize) {
        this(null, comparator, maxSize);
    }

    public FixedSizePriorityQueueDeserializer(Class<?> vc, Comparator<ProductScore> comparator, int maxSize) {
        super(vc);
        this.comparator = comparator;
        this.maxSize = maxSize;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public FixedSizePriorityQueue<ProductScore> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        FixedSizePriorityQueue<ProductScore> queue = new FixedSizePriorityQueue<>(comparator, maxSize);
        JsonNode json = p.getCodec().readTree(p);
        Iterator<JsonNode> i = json.elements();
        while (i.hasNext()) {
            JsonNode node = i.next();
            String productId = node.get("productId").asText();
            int likes = node.get("score").asInt();
            ProductScore productLikes = new ProductScore.Builder(productId).score(likes).build();
            queue.add(productLikes);
        }
        return queue;
    }
}
