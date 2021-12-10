package no.cantara.messi.kinesis;

import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiShard;
import no.cantara.messi.api.MessiStreamingConsumer;
import no.cantara.messi.protos.MessiMessage;
import no.cantara.messi.protos.MessiProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class KinesisMessiShard implements MessiShard {

    private static Logger log = LoggerFactory.getLogger(KinesisMessiShard.class);

    static final AtomicInteger nextScheduledThreadId = new AtomicInteger();
    static final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(2, runnable ->
            new Thread(runnable, "scheduled-kinesis-consumer-" + nextScheduledThreadId.incrementAndGet()));

    final KinesisMessiTopic messiTopic;
    final String shardId;
    final KinesisAsyncClient kinesisAsyncClient;
    final String streamName;
    final String topicName;

    final CopyOnWriteArrayList<KinesisMessiStreamingConsumer> consumers = new CopyOnWriteArrayList<>();

    final AtomicBoolean closed = new AtomicBoolean();

    public KinesisMessiShard(KinesisMessiTopic messiTopic, String shardId, KinesisAsyncClient kinesisAsyncClient, String streamName, String topicName) {
        this.messiTopic = messiTopic;
        this.shardId = shardId;
        this.kinesisAsyncClient = kinesisAsyncClient;
        this.streamName = streamName;
        this.topicName = topicName;
    }

    @Override
    public boolean supportsStreaming() {
        return true;
    }

    @Override
    public MessiStreamingConsumer streamingConsumer(MessiCursor cursor) {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        if (!(cursor instanceof KinesisMessiCursor)) {
            throw new IllegalArgumentException("cursor must be of type " + KinesisMessiCursor.class.getName());
        }
        KinesisMessiCursor initialPosition = (KinesisMessiCursor) cursor;
        KinesisStreamingBuffer kinesisConsumerBuffer = new KinesisStreamingBuffer(scheduledExecutor, kinesisAsyncClient, streamName, initialPosition.shardId, 1000, Duration.of(1, ChronoUnit.MINUTES), initialPosition);
        KinesisMessiStreamingConsumer consumer = new KinesisMessiStreamingConsumer(this, kinesisConsumerBuffer, topicName, initialPosition, 1000);
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public KinesisMessiCursor.Builder cursorOf() {
        return new KinesisMessiCursor.Builder()
                .shardId(shardId);
    }

    @Override
    public KinesisMessiCursor cursorOfCheckpoint(String checkpoint) {
        KinesisMessiCursor cursor = new KinesisMessiCursor.Builder()
                .checkpoint(checkpoint)
                .build();
        return cursor;
    }

    @Override
    public KinesisMessiCursor cursorAt(MessiMessage messiMessage) {
        Objects.requireNonNull(messiMessage);
        if (!messiMessage.hasProvider()) {
            throw new IllegalArgumentException("Provided message does not have 'provider'");
        }
        MessiProvider provider = messiMessage.getProvider();
        if (!provider.hasShardId()) {
            throw new IllegalArgumentException("Provided message does not have 'provider.shardId'");
        }
        if (!provider.hasPublishedTimestamp()) {
            throw new IllegalArgumentException("Provided message does not have 'provider.publishedTimestamp'");
        }
        if (!provider.hasSequenceNumber()) {
            throw new IllegalArgumentException("Provided message does not have 'provider.sequenceNumber'");
        }
        if (!shardId.equals(provider.getShardId())) {
            throw new IllegalArgumentException("The 'provider.shardId' of provided message does not match the shardId of the consumer");
        }
        return new KinesisMessiCursor.Builder()
                .shardId(shardId)
                .providerTimestamp(Instant.ofEpochMilli(provider.getPublishedTimestamp()))
                .providerSequenceNumber(provider.getSequenceNumber())
                .inclusive(true)
                .build();
    }

    @Override
    public KinesisMessiCursor cursorAfter(MessiMessage messiMessage) {
        Objects.requireNonNull(messiMessage);
        if (!messiMessage.hasProvider()) {
            throw new IllegalArgumentException("Provided message does not have 'provider'");
        }
        MessiProvider provider = messiMessage.getProvider();
        if (!provider.hasShardId()) {
            throw new IllegalArgumentException("Provided message does not have 'provider.shardId'");
        }
        if (!provider.hasPublishedTimestamp()) {
            throw new IllegalArgumentException("Provided message does not have 'provider.publishedTimestamp'");
        }
        if (!provider.hasSequenceNumber()) {
            throw new IllegalArgumentException("Provided message does not have 'provider.sequenceNumber'");
        }
        if (!shardId.equals(provider.getShardId())) {
            throw new IllegalArgumentException("The 'provider.shardId' of provided message does not match the shardId of the consumer");
        }
        return new KinesisMessiCursor.Builder()
                .shardId(shardId)
                .providerTimestamp(Instant.ofEpochMilli(provider.getPublishedTimestamp()))
                .providerSequenceNumber(provider.getSequenceNumber())
                .inclusive(false)
                .build();
    }

    @Override
    public KinesisMessiCursor cursorAtLastMessage() throws MessiClosedException {
        throw new UnsupportedOperationException("Kinesis does not support putting cursor at last-message, only after/now. Use timestamp based cursor instead");
    }

    @Override
    public KinesisMessiCursor cursorAfterLastMessage() throws MessiClosedException {
        return cursorOf()
                .now()
                .build();
    }

    @Override
    public KinesisMessiCursor cursorHead() {
        return cursorOf()
                .now()
                .build();
    }

    @Override
    public KinesisMessiCursor cursorAtTrimHorizon() {
        return cursorOf()
                .oldest()
                .build();
    }

    @Override
    public KinesisMessiTopic topic() {
        return messiTopic;
    }

    @Override
    public void close() {
        closed.set(true);
        for (KinesisMessiStreamingConsumer consumer : consumers) {
            consumer.close();
        }
        consumers.clear();
    }
}
