package org.globex.retail.streams.serde;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.globex.retail.streams.collectors.FixedSizePriorityQueue;
import org.globex.retail.streams.model.ProductLikes;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

public class FixedSizePriorityQueueSerDeTest {

    @Test
    public void testSerializeAndDeserialize() throws JsonProcessingException {
        Comparator<ProductLikes> comparator = (pl1, pl2) -> pl2.getLikes() - pl1.getLikes();
        FixedSizePriorityQueue<ProductLikes> queue = new FixedSizePriorityQueue<>(comparator, 5);
        queue.add(new ProductLikes.Builder("123456").likes(10).build());
        queue.add(new ProductLikes.Builder("234567").likes(5).build());
        queue.add(new ProductLikes.Builder("345678").likes(15).build());
        ObjectMapper mapper = new ObjectMapper();
        String serialized = mapper.writeValueAsString(queue);
        FixedSizePriorityQueue<ProductLikes> fromJson = new ObjectMapper().readValue(serialized, FixedSizePriorityQueue.class);
        MatcherAssert.assertThat(fromJson.size(), Matchers.equalTo(3));
        List<String> products = new ArrayList<>();
        List<Integer> likes = new ArrayList<>();
        Iterator<ProductLikes> i = fromJson.iterator();
        while (i.hasNext()) {
            ProductLikes p = i.next();
            products.add(p.getProductId());
            likes.add(p.getLikes());
        }
        MatcherAssert.assertThat(products.get(0), Matchers.equalTo("345678"));
        MatcherAssert.assertThat(products.get(1), Matchers.equalTo("123456"));
        MatcherAssert.assertThat(products.get(2), Matchers.equalTo("234567"));
        MatcherAssert.assertThat(likes.get(0), Matchers.equalTo(15));
        MatcherAssert.assertThat(likes.get(1), Matchers.equalTo(10));
        MatcherAssert.assertThat(likes.get(2), Matchers.equalTo(5));
    }

}
