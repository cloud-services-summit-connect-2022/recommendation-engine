package org.globex.retail.streams.serde;

import java.io.IOException;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.globex.retail.streams.collectors.FixedSizePriorityQueue;
import org.globex.retail.streams.model.ProductLikes;

public class FixedSizePriorityQueueSerializer extends StdSerializer<FixedSizePriorityQueue<ProductLikes>> {

    public FixedSizePriorityQueueSerializer() {
        this(null);
    }

    public FixedSizePriorityQueueSerializer(Class<FixedSizePriorityQueue<ProductLikes>> t) {
        super(t);
    }

    @Override
    public void serialize(FixedSizePriorityQueue<ProductLikes> queue, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeStartArray();
        Iterator<ProductLikes> iterator = queue.iterator();
        while (iterator.hasNext()) {
            ProductLikes productLikes = iterator.next();
            jsonGenerator.writeObject(productLikes);
        }
        jsonGenerator.writeEndArray();
    }
}
