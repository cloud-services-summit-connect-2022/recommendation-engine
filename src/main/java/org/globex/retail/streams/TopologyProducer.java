package org.globex.retail.streams;

import java.util.Comparator;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.globex.retail.streams.collectors.FixedSizePriorityQueue;
import org.globex.retail.streams.model.ProductLikes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
@ApplicationScoped
public class TopologyProducer {

    private static final Logger log = LoggerFactory.getLogger(TopologyProducer.class);

    @ConfigProperty(name = "aggregation.size")
    int aggregationSize;

    @ConfigProperty(name = "tracking-event-topic")
    String trackingEventTopic;

    @ConfigProperty(name = "aggregation.store")
    String aggregationStore;

    @Produces
    public Topology buildTopology() {

        final ObjectMapperSerde<ProductLikes> productLikesSerde = new ObjectMapperSerde<>(ProductLikes.class);
        final ObjectMapperSerde<FixedSizePriorityQueue> fixedSizePriorityQueueSerde = new ObjectMapperSerde<>(FixedSizePriorityQueue.class);

        Comparator<ProductLikes> comparator = (pl1, pl2) -> pl2.getLikes() - pl1.getLikes();
        FixedSizePriorityQueue<ProductLikes> fixedQueue = new FixedSizePriorityQueue<>(comparator, aggregationSize);

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, ProductLikes> productLikes =
                builder.stream(trackingEventTopic, Consumed.with(Serdes.String(), Serdes.String()))
                        .mapValues(value -> {
                            JsonObject activity = new JsonObject(value);
                            String productId = activity.getJsonObject("actionInfo").getString("productId");
                            return new ProductLikes.Builder(productId).build();
                        }).groupBy((key, value) -> value.getProductId(), Grouped.with(Serdes.String(), productLikesSerde))
                        .reduce(ProductLikes::sum);

        productLikes.groupBy((key, value) -> KeyValue.pair(value.getCategory(), value), Grouped.with(Serdes.String(), productLikesSerde))
                .aggregate(() -> fixedQueue,
                        (key, value, aggregate) -> aggregate.add(value),
                        (key, value, aggregate) -> aggregate.remove(value),
                        Materialized.<String, FixedSizePriorityQueue, KeyValueStore<Bytes, byte[]>>as(aggregationStore).withKeySerde(Serdes.String()).withValueSerde(fixedSizePriorityQueueSerde));

        Topology topology = builder.build();
        log.debug(topology.describe().toString());
        return topology;
    }
}
