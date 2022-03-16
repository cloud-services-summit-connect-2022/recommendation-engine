package org.globex.retail.streams.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

public class ProductLikesTest {

    @Test
    public void testSerializeToJson() throws JsonProcessingException {
        ProductLikes productLikes = new ProductLikes.Builder("123456").build();
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonProductLike = objectMapper.writeValueAsString(productLikes);
        ProductLikes fromJson = objectMapper.readValue(jsonProductLike, ProductLikes.class);
        MatcherAssert.assertThat(fromJson.getProductId(), Matchers.equalTo("123456"));
        MatcherAssert.assertThat(fromJson.getLikes(), Matchers.equalTo(1));
    }

}
