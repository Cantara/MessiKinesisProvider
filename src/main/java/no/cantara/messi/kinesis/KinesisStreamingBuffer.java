package no.cantara.messi.kinesis;

import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;
import no.cantara.messi.protos.MessiProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class KinesisStreamingBuffer implements AutoCloseable {

    private static Logger log = LoggerFactory.getLogger(KinesisStreamingBuffer.class);

    static final int LIMIT = 1000;

    final AtomicBoolean closed = new AtomicBoolean();
    final ScheduledExecutorService scheduledExecutor;
    final KinesisAsyncClient kinesisAsyncClient;
    final String streamName;
    final String shardId;
    final int pollIntervalMs;
    final Duration atUlidTimestampTolerance;
    final AtomicLong kinesisRequestsCount = new AtomicLong();
    final AtomicLong kinesisResponesHandledCount = new AtomicLong();
    final AtomicLong totalMessagesFetchedCount = new AtomicLong();

    final BlockingQueue<MessiMessage> messageBuffer = new ArrayBlockingQueue<>(2 * LIMIT);

    final Object lock = new Object(); // protects members that are not thread-safe
    String nextShardIterator;
    GetRecordsRequest pendingRecordsRequest;
    CompletableFuture<GetRecordsResponse> pendingRecordsResponse;
    ScheduledFuture<?> scheduledFuture; // next task that will run after delay

    KinesisStreamingBuffer(ScheduledExecutorService scheduledExecutor, KinesisAsyncClient kinesisAsyncClient, String streamName, String shardId, int pollIntervalMs, Duration atUlidTimestampTolerance, KinesisMessiCursor initialPosition) {
        this.scheduledExecutor = scheduledExecutor;
        this.kinesisAsyncClient = kinesisAsyncClient;
        this.streamName = streamName;
        this.shardId = shardId;
        this.pollIntervalMs = pollIntervalMs;
        this.atUlidTimestampTolerance = atUlidTimestampTolerance;
        this.nextShardIterator = getShardIterator(streamName, initialPosition);
    }

    public MessiMessage poll(int timeout, TimeUnit timeUnit) throws InterruptedException {
        if (closed.get()) {
            throw new IllegalStateException("KinesisConsumerBuffer is closed");
        }
        return messageBuffer.poll(timeout, timeUnit);
    }

    public void triggerAsyncFill() {
        if (closed.get()) {
            return;
        }
        synchronized (lock) {
            try {
                if (pendingRecordsRequest != null) {
                    // there is already a pending request that is not yet completed
                    return;
                }
                if (messageBuffer.size() >= LIMIT) {
                    // there is not enough space in buffer to fill yet
                    return;
                }

                // no pending requests and there is available space in buffer

                pendingRecordsRequest = GetRecordsRequest.builder()
                        .shardIterator(nextShardIterator)
                        .limit(LIMIT)
                        .build();

                pendingRecordsResponse = kinesisAsyncClient.getRecords(pendingRecordsRequest)
                        .handle(this::handleGetRecordsResponse);

                kinesisRequestsCount.incrementAndGet();
            } catch (Throwable t) {
                log.error("While attempting to trigger Kinesis getRecords", t);
            }
        }
    }

    private GetRecordsResponse handleGetRecordsResponse(GetRecordsResponse response, Throwable throwable) {
        synchronized (lock) {
            try {
                if (closed.get()) {
                    return response;
                }
                try {
                    if (throwable != null) {
                        log.error(String.format("While handling Kinesis get-records response. shardId=%s, shardIterator=%s",
                                shardId, pendingRecordsRequest.shardIterator()), throwable);
                        return response;
                    }
                    if (!response.sdkHttpResponse().isSuccessful()) {
                        log.error("While processing http response from getting Kinesis records. shardId={}, shardIterator={}, statusCode={}, statusText={}",
                                shardId, pendingRecordsRequest.shardIterator(), response.sdkHttpResponse().statusCode(), response.sdkHttpResponse().statusText());
                        return response;
                    }

                    if (!response.hasRecords() || response.records().size() == 0) {
                        // no records consumed, Kinesis stream has no more records

                        // attempt to read more messages again after a small delay
                        scheduledFuture = scheduledExecutor.schedule(this::triggerAsyncFill, pollIntervalMs, TimeUnit.MILLISECONDS);

                        return response;
                    }

                    List<Record> records = response.records();

                    log.trace("Got {} more records from Kinesis, filling buffer", records.size());

                    List<MessiMessage> messages = new ArrayList<>(records.size());

                    for (Record record : records) {
                        String sequenceNumber = record.sequenceNumber();
                        ZonedDateTime approximateArrivalTimestamp = ZonedDateTime.ofInstant(record.approximateArrivalTimestamp(), ZoneId.of("Europe/Oslo"));
                        String partitionKey = record.partitionKey();
                        SdkBytes data = record.data();

                        MessiMessage messiMessage = MessiMessage.newBuilder()
                                .mergeFrom(data.asByteArrayUnsafe())
                                .setProvider(MessiProvider.newBuilder()
                                        .setPublishedTimestamp(record.approximateArrivalTimestamp().toEpochMilli())
                                        .setShardId(shardId)
                                        .setSequenceNumber(sequenceNumber)
                                        .build())
                                .build();

                        messages.add(messiMessage);

                        log.trace("Got record from Kinesis. seq={}, arrival-time={}, partitionKey={}, message-ulid: {}",
                                sequenceNumber, approximateArrivalTimestamp, partitionKey, MessiULIDUtils.toUlid(messiMessage.getUlid()));
                    }


                    boolean wasAdded = messageBuffer.addAll(messages);

                    if (!wasAdded) {
                        // should never happen, as the request will never be sent to Kinesis unless there was space
                        throw new IllegalStateException("Not enough space in messageBuffer to add message!");
                    }

                    totalMessagesFetchedCount.addAndGet(messages.size());

                    log.trace("Added messages to buffer: count={}", messages.size());

                    nextShardIterator = response.nextShardIterator();

                    return response;

                } catch (Throwable handlingThrowable) {
                    log.error(String.format("Unexpected error while handling Kinesis request. shardId=%s, shardIterator=%s",
                            shardId, pendingRecordsRequest.shardIterator()), handlingThrowable);

                    return response;
                }
            } finally {
                kinesisResponesHandledCount.incrementAndGet();

                // clear pending request
                pendingRecordsRequest = null;
            }
        }
    }

    private String getShardIterator(String streamName, KinesisMessiCursor cursor) {
        Objects.requireNonNull(cursor.shardId);
        GetShardIteratorRequest.Builder shardIteratorRequestBuilder = GetShardIteratorRequest.builder()
                .streamName(streamName)
                .shardId(cursor.shardId);
        switch (cursor.type) {
            case AT_PROVIDER_SEQUENCE:
                if (cursor.inclusive) {
                    log.debug("Starting kinesis consumer at sequence (AT_SEQUENCE_NUMBER): {}", cursor.sequenceNumber);
                    shardIteratorRequestBuilder.shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER);
                } else {
                    log.debug("Starting kinesis consumer after sequence (AFTER_SEQUENCE_NUMBER): {}", cursor.sequenceNumber);
                    shardIteratorRequestBuilder.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
                }
                shardIteratorRequestBuilder.startingSequenceNumber(cursor.sequenceNumber);
                break;
            case AT_PROVIDER_TIME:
                log.debug("Starting kinesis consumer at time (AT_TIMESTAMP): {}", cursor.timestamp);
                shardIteratorRequestBuilder.shardIteratorType(ShardIteratorType.AT_TIMESTAMP);
                shardIteratorRequestBuilder.timestamp(cursor.timestamp);
                break;
            case NOW:
                log.debug("Starting kinesis consumer now, after the most recent records (LATEST)");
                shardIteratorRequestBuilder.shardIteratorType(ShardIteratorType.LATEST);
                break;
            case OLDEST_RETAINED:
                log.debug("Starting kinesis consumer from oldest retained message (TRIM_HORIZON)");
                shardIteratorRequestBuilder.shardIteratorType(ShardIteratorType.TRIM_HORIZON);
                break;
            case AT_ULID:
                log.debug("Starting kinesis consumer from ulid time (AT_TIMESTAMP) minus one-minute: {}", cursor.ulid);
                shardIteratorRequestBuilder.shardIteratorType(ShardIteratorType.AT_TIMESTAMP);
                shardIteratorRequestBuilder.timestamp(Instant.ofEpochMilli(cursor.ulid.timestamp()).minus(atUlidTimestampTolerance));
                break;
            case AT_EXTERNAL_ID:
                log.debug("Starting kinesis consumer looking for externalId '{}' at approximately (AT_TIMESTAMP) {}, tolerating {} seconds", cursor.externalId, cursor.externalIdTimestamp, cursor.externalIdTimestampTolerance.getSeconds());
                shardIteratorRequestBuilder.shardIteratorType(ShardIteratorType.AT_TIMESTAMP);
                shardIteratorRequestBuilder.timestamp(cursor.externalIdTimestamp.minus(cursor.externalIdTimestampTolerance));
                break;
        }
        GetShardIteratorRequest shardIteratorRequest = shardIteratorRequestBuilder
                .build();
        GetShardIteratorResponse getShardIteratorResponse = kinesisAsyncClient.getShardIterator(shardIteratorRequest).join();
        if (!getShardIteratorResponse.sdkHttpResponse().isSuccessful()) {
            throw new RuntimeException(String.format("Did not succeed in getting shard-iterator from Kinesis stream '%s'. statusCode=%s, text: '%s'",
                    streamName,
                    getShardIteratorResponse.sdkHttpResponse().statusCode(),
                    getShardIteratorResponse.sdkHttpResponse().statusText().orElse("")));
        }
        String shardIterator = getShardIteratorResponse.shardIterator(); // initial iterator
        return shardIterator;
    }

    public void seek(long timestamp) {
        if (closed.get()) {
            throw new MessiClosedException();
        }

        KinesisMessiCursor cursor = new KinesisMessiCursor.Builder()
                .shardId(shardId)
                .providerTimestamp(Instant.ofEpochMilli(timestamp))
                .inclusive(true)
                .build();


        boolean isPending;
        CompletableFuture<GetRecordsResponse> pendingRecordsResponse;
        synchronized (lock) {
            isPending = pendingRecordsRequest != null;
            pendingRecordsResponse = this.pendingRecordsResponse;
        }
        if (isPending) {
            // in the case of a false positive - due to race condition - the following join statement should
            pendingRecordsResponse.join(); // wait for response of pending request to complete - join must not be called in synchronized block to avoid potential deadlock
        }

        messageBuffer.clear();

        this.nextShardIterator = getShardIterator(streamName, cursor);

        triggerAsyncFill();
    }

    @Override
    public void close() {
        closed.set(true);
        synchronized (lock) {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }
        }
    }
}
