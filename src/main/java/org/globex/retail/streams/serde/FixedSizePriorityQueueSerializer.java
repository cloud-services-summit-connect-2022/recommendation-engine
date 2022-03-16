package org.globex.retail.streams.serde;

import java.io.IOException;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.globex.retail.streams.collectors.FixedSizePriorityQueue;
import org.globex.retail.streams.model.ProductScore;

public class FixedSizePriorityQueueSerializer extends StdSerializer<FixedSizePriorityQueue<ProductScore>> {

    public FixedSizePriorityQueueSerializer() {
        this(null);
    }

    public FixedSizePriorityQueueSerializer(Class<FixedSizePriorityQueue<ProductScore>> t) {
        super(t);
    }

    @Override
    public void serialize(FixedSizePriorityQueue<ProductScore> queue, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeStartArray();
        Iterator<ProductScore> iterator = queue.iterator();
        while (iterator.hasNext()) {
            ProductScore productLikes = iterator.next();
            jsonGenerator.writeObject(productLikes);
        }
        jsonGenerator.writeEndArray();
    }
}
