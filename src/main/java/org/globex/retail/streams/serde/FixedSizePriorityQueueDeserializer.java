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
import org.globex.retail.streams.model.ProductLikes;

public class FixedSizePriorityQueueDeserializer extends StdDeserializer<FixedSizePriorityQueue<ProductLikes>> {

    public FixedSizePriorityQueueDeserializer() {
        this(null);
    }

    public FixedSizePriorityQueueDeserializer(Class<?> vc) {
        super(vc);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public FixedSizePriorityQueue<ProductLikes> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        Comparator<ProductLikes> comparator = (pl1, pl2) -> pl2.getLikes() - pl1.getLikes();
        FixedSizePriorityQueue queue = new FixedSizePriorityQueue(comparator);
        JsonNode json = p.getCodec().readTree(p);
        Iterator<JsonNode> i = json.elements();
        while (i.hasNext()) {
            JsonNode node = i.next();
            String productId = node.get("productId").asText();
            int likes = node.get("likes").asInt();
            ProductLikes productLikes = new ProductLikes.Builder(productId).likes(likes).build();
            queue.add(productLikes);
        }
        return queue;
    }
}
