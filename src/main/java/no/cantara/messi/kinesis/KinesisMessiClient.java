package no.cantara.messi.kinesis;

import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiMetadataClient;
import no.cantara.messi.protos.MessiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;

import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class KinesisMessiClient implements MessiClient {

    private static Logger log = LoggerFactory.getLogger(KinesisMessiClient.class);

    final KinesisAsyncClient kinesisAsyncClient;
    final String streamName;
    final Path kinesisCheckpointsFolder;

    final AtomicBoolean closed = new AtomicBoolean();
    final CopyOnWriteArrayList<KinesisMessiConsumer> consumers = new CopyOnWriteArrayList<>();
    final CopyOnWriteArrayList<KinesisMessiProducer> producers = new CopyOnWriteArrayList<>();

    static final AtomicInteger nextScheduledThreadId = new AtomicInteger();
    static final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(2, runnable ->
            new Thread(runnable, "scheduled-kinesis-consumer-" + nextScheduledThreadId.incrementAndGet()));

    public KinesisMessiClient(KinesisAsyncClient kinesisAsyncClient, String streamName, Path kinesisCheckpointsFolder) {
        this.kinesisAsyncClient = kinesisAsyncClient;
        this.streamName = streamName;
        this.kinesisCheckpointsFolder = kinesisCheckpointsFolder;
    }

    @Override
    public KinesisMessiProducer producer(String topic) {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        return new KinesisMessiProducer(kinesisAsyncClient, streamName, topic);
    }

    @Override
    public KinesisMessiConsumer consumer(String topic, MessiCursor cursor) {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        if (cursor == null) {
            List<String> shards = shards(topic);
            if (shards == null || shards.isEmpty()) {
                throw new IllegalStateException(String.format("Topic contains no shards. streamName: %s, topic: %s", streamName, topic));
            }
            String shardId = shards.get(0);
            cursor = new KinesisMessiCursor.Builder()
                    .shardId(shardId)
                    .oldest()
                    .build();
        } else if (!(cursor instanceof KinesisMessiCursor)) {
            throw new IllegalArgumentException("cursor must be of type " + KinesisMessiCursor.class.getName());
        }
        KinesisMessiCursor initialPosition = (KinesisMessiCursor) cursor;
        KinesisStreamingBuffer kinesisConsumerBuffer = new KinesisStreamingBuffer(scheduledExecutor, kinesisAsyncClient, streamName, initialPosition.shardId, 1000, Duration.of(1, ChronoUnit.MINUTES), initialPosition);
        return new KinesisMessiConsumer(kinesisConsumerBuffer, topic, initialPosition, 1000);
    }

    @Override
    public MessiCursor.Builder cursorOf() {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        return new KinesisMessiCursor.Builder();
    }

    @Override
    public MessiMessage lastMessage(String topic, String shardId) throws MessiClosedException {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public List<String> shards(String topic) {
        // ignore provided topic, and use configured streamName instead. Topic is used to multiplex data within the stream.
        StreamDescription streamDescription = KinesisUtils.waitForStreamToBecomeAvailable(kinesisAsyncClient, streamName);
        List<Shard> shards = streamDescription.shards();
        List<String> shardNames = shards.stream().map(Shard::shardId).collect(Collectors.toList());
        return shardNames;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closed.set(true);
        try {
            for (KinesisMessiConsumer consumer : consumers) {
                consumer.close();
            }
            consumers.clear();
            for (KinesisMessiProducer producer : producers) {
                producer.close();
            }
            producers.clear();
        } finally {
            kinesisAsyncClient.close();
        }
    }

    @Override
    public MessiMetadataClient metadata(String topic) {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        throw new UnsupportedOperationException("Kinesis provider does not support metadata-client");
    }
}
