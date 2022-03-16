package org.globex.retail.streams.serde;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.globex.retail.streams.collectors.FixedSizePriorityQueue;
import org.globex.retail.streams.model.ProductScore;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

public class FixedSizePriorityQueueSerDeTest {

    @Test
    public void testSerializeAndDeserialize() throws JsonProcessingException {
        Comparator<ProductScore> comparator = (pl1, pl2) -> pl2.getScore() - pl1.getScore();
        FixedSizePriorityQueue<ProductScore> queue = new FixedSizePriorityQueue<>(comparator, 5);
        queue.add(new ProductScore.Builder("123456").score(10).build());
        queue.add(new ProductScore.Builder("234567").score(5).build());
        queue.add(new ProductScore.Builder("345678").score(15).build());
        ObjectMapper mapper = new ObjectMapper();
        String serialized = mapper.writeValueAsString(queue);
        FixedSizePriorityQueue<ProductScore> fromJson = new ObjectMapper().readValue(serialized, FixedSizePriorityQueue.class);
        MatcherAssert.assertThat(fromJson.size(), Matchers.equalTo(3));
        List<String> products = new ArrayList<>();
        List<Integer> likes = new ArrayList<>();
        Iterator<ProductScore> i = fromJson.iterator();
        while (i.hasNext()) {
            ProductScore p = i.next();
            products.add(p.getProductId());
            likes.add(p.getScore());
        }
        MatcherAssert.assertThat(products.get(0), Matchers.equalTo("345678"));
        MatcherAssert.assertThat(products.get(1), Matchers.equalTo("123456"));
        MatcherAssert.assertThat(products.get(2), Matchers.equalTo("234567"));
        MatcherAssert.assertThat(likes.get(0), Matchers.equalTo(15));
        MatcherAssert.assertThat(likes.get(1), Matchers.equalTo(10));
        MatcherAssert.assertThat(likes.get(2), Matchers.equalTo(5));
    }

}
