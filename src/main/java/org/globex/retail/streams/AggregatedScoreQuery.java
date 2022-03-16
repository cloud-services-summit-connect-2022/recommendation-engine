package org.globex.retail.streams;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.globex.retail.streams.collectors.FixedSizePriorityQueue;
import org.globex.retail.streams.model.ProductScore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class AggregatedScoreQuery {

    private static final Logger log = LoggerFactory.getLogger(AggregatedScoreQuery.class);

    @Inject
    KafkaStreams streams;

    @ConfigProperty(name = "aggregation.store")
    String aggregationStore;

    public Uni<String> getAggregatedScore(String category) {

        return Uni.createFrom().item(() -> store().get(category))
                .onItem().transform(queue -> {
                    if (queue == null) {
                        log.info("No data found for key: " + category);
                        return null;
                    } else {
                        JsonArray array = new JsonArray();
                        queue.iterator().forEachRemaining(productScore -> array.add(new JsonObject().put("productId", productScore.getProductId())
                                .put("score", productScore.getScore())));
                        return array.encode();
                    }
                });
    }

    private ReadOnlyKeyValueStore<String, FixedSizePriorityQueue<ProductScore>> store() {
        while (true) {
            try {
                return streams.store(StoreQueryParameters.fromNameAndType(aggregationStore, QueryableStoreTypes.keyValueStore()));
            } catch (InvalidStateStoreException e) {
                // ignore, store not ready yet
            }
        }
    }

}
