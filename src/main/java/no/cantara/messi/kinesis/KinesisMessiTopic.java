package no.cantara.messi.kinesis;

import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiMetadataClient;
import no.cantara.messi.api.MessiProducer;
import no.cantara.messi.api.MessiShard;
import no.cantara.messi.api.MessiTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class KinesisMessiTopic implements MessiTopic {

    private static Logger log = LoggerFactory.getLogger(KinesisMessiTopic.class);

    final KinesisMessiClient messiClient;
    final String name;
    final KinesisAsyncClient kinesisAsyncClient;
    final String streamName;
    final int pollIntervalMs;

    final AtomicBoolean closed = new AtomicBoolean();
    final Map<String, KinesisMessiShard> shardById = new ConcurrentHashMap<>();
    final CopyOnWriteArrayList<KinesisMessiProducer> producers = new CopyOnWriteArrayList<>();

    public KinesisMessiTopic(KinesisMessiClient messiClient, String name, KinesisAsyncClient kinesisAsyncClient, String streamName, int pollIntervalMs) {
        this.messiClient = messiClient;
        this.name = name;
        this.kinesisAsyncClient = kinesisAsyncClient;
        this.streamName = streamName;
        this.pollIntervalMs = pollIntervalMs;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public MessiProducer producer() {
        if (closed.get()) {
            throw new MessiClosedException();
        }

        KinesisMessiProducer producer = new KinesisMessiProducer(kinesisAsyncClient, streamName, name);
        producers.add(producer);
        return producer;
    }

    @Override
    public List<String> shards() {
        // ignore topic-name, and use configured streamName instead. Topic is used to multiplex data within the stream.
        StreamDescription streamDescription = KinesisUtils.waitForStreamToBecomeAvailable(kinesisAsyncClient, streamName);
        List<Shard> shards = streamDescription.shards();
        List<String> shardNames = shards.stream().map(Shard::shardId).collect(Collectors.toList());
        return shardNames;
    }

    @Override
    public String firstShard() {
        List<String> shards = shards();
        if (shards.isEmpty()) {
            throw new IllegalStateException("No shards present in Kinesis stream.");
        }
        return shards.get(0);
    }

    @Override
    public MessiShard shardOf(String shardId) {
        return shardById.computeIfAbsent(shardId, sid -> new KinesisMessiShard(this, sid, kinesisAsyncClient, streamName, name, pollIntervalMs));
    }

    @Override
    public MessiMetadataClient metadata() {
        throw new UnsupportedOperationException();
    }

    @Override
    public KinesisMessiClient client() {
        return messiClient;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closed.set(true);
        for (KinesisMessiProducer producer : producers) {
            producer.close();
        }
        producers.clear();
        for (KinesisMessiShard shard : shardById.values()) {
            try {
                shard.close();
            } catch (Exception e) {
                log.warn("", e);
            }
        }
        shardById.clear();
    }
}
