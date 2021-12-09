package no.cantara.messi.kinesis;

import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiCursorStartingPointType;
import no.cantara.messi.api.MessiStreamingConsumer;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class KinesisMessiStreamingConsumer implements MessiStreamingConsumer {

    private static Logger log = LoggerFactory.getLogger(KinesisMessiStreamingConsumer.class);

    static final Duration AT_ULID_TIMESTAMP_TOLERANCE = Duration.of(1, ChronoUnit.MINUTES);

    final AtomicBoolean closed = new AtomicBoolean();
    final String streamName;
    final AtomicReference<KinesisStreamingBuffer> kinesisConsumerBufferRef = new AtomicReference<>();
    final String shardId;
    final int pollIntervalMs;
    final KinesisMessiCursor initialPosition;
    final AtomicBoolean initialPositionReached = new AtomicBoolean();
    final AtomicBoolean initialBufferingEnabled = new AtomicBoolean();
    final BlockingDeque<MessiMessage> initialPositionLookaheadBuffer = new LinkedBlockingDeque<>();

    public KinesisMessiStreamingConsumer(KinesisStreamingBuffer kinesisStreamingBuffer, String streamName, KinesisMessiCursor initialPosition, int pollIntervalMs) {
        this.streamName = streamName;
        this.shardId = initialPosition.shardId;
        this.pollIntervalMs = pollIntervalMs;
        this.initialPosition = initialPosition;
        this.kinesisConsumerBufferRef.set(kinesisStreamingBuffer);
    }

    @Override
    public String topic() {
        return streamName;
    }

    @Override
    public MessiMessage receive(int timeout, TimeUnit timeUnit) throws InterruptedException, MessiClosedException {
        if (closed.get()) {
            throw new MessiClosedException();
        }

        long expireTime = System.currentTimeMillis() + timeUnit.toMillis(timeout);

        KinesisStreamingBuffer kinesisStreamingBuffer = kinesisConsumerBufferRef.get();

        if ((initialPosition.type == MessiCursorStartingPointType.AT_EXTERNAL_ID
                || initialPosition.type == MessiCursorStartingPointType.AT_ULID)
                && !initialPositionReached.get()) {

            kinesisStreamingBuffer.triggerAsyncFill();

            MessiMessage messiMessage = null;
            while (kinesisStreamingBuffer.kinesisResponesHandledCount.get() == 0 && messiMessage == null) {
                messiMessage = kinesisStreamingBuffer.poll(200, TimeUnit.MILLISECONDS);
            }
            while (messiMessage == null) {
                if (System.currentTimeMillis() > expireTime) {
                    return null;
                }
                kinesisStreamingBuffer.triggerAsyncFill();
                messiMessage = kinesisStreamingBuffer.poll(200, TimeUnit.MILLISECONDS);
            }
            do {
                if (initialPosition.type == MessiCursorStartingPointType.AT_ULID) {

                    if (initialPosition.ulid.equals(MessiULIDUtils.toUlid(messiMessage.getUlid()))) {
                        initialPositionLookaheadBuffer.clear();
                        initialPositionReached.set(true);
                        if (initialPosition.inclusive) {
                            return messiMessage;
                        } else {
                            break;
                        }
                    }

                    if (initialBufferingEnabled.get()) {
                        initialPositionLookaheadBuffer.add(messiMessage);
                    } else if (initialPosition.ulid.timestamp() <= MessiULIDUtils.toUlid(messiMessage.getUlid()).timestamp()) {
                        initialBufferingEnabled.set(true);
                        initialPositionLookaheadBuffer.add(messiMessage);
                    }

                    Instant upperBound = Instant.ofEpochMilli(initialPosition.ulid.timestamp()).plus(AT_ULID_TIMESTAMP_TOLERANCE);
                    if (upperBound.toEpochMilli() < MessiULIDUtils.toUlid(messiMessage.getUlid()).timestamp()) {
                        initialPositionReached.set(true);
                        break;
                    }

                } else if (initialPosition.type == MessiCursorStartingPointType.AT_EXTERNAL_ID) {

                    if (initialPosition.externalId.equals(messiMessage.getExternalId())) {
                        initialPositionLookaheadBuffer.clear();
                        initialPositionReached.set(true);
                        if (initialPosition.inclusive) {
                            return messiMessage;
                        } else {
                            break;
                        }
                    }

                    if (initialBufferingEnabled.get()) {
                        initialPositionLookaheadBuffer.add(messiMessage);
                    } else {
                        Instant lowerBound = initialPosition.externalIdTimestamp.minus(initialPosition.externalIdTimestampTolerance);
                        if (lowerBound.toEpochMilli() <= MessiULIDUtils.toUlid(messiMessage.getUlid()).timestamp()) {
                            initialBufferingEnabled.set(true);
                            initialPositionLookaheadBuffer.add(messiMessage);
                        }
                    }

                    Instant upperBound = initialPosition.externalIdTimestamp.plus(initialPosition.externalIdTimestampTolerance);
                    if (upperBound.toEpochMilli() < MessiULIDUtils.toUlid(messiMessage.getUlid()).timestamp()) {
                        initialPositionReached.set(true);
                        break;
                    }
                }

                kinesisStreamingBuffer.triggerAsyncFill();
                messiMessage = kinesisStreamingBuffer.poll(200, TimeUnit.MILLISECONDS);

            } while (messiMessage != null);
        }

        if (initialBufferingEnabled.get()) {
            MessiMessage messiMessage = initialPositionLookaheadBuffer.poll();
            if (messiMessage != null) {
                return messiMessage;
            } else {
                initialBufferingEnabled.set(false);
            }
        }

        /*
         * Attempt to get a message from the buffer within time interval
         */

        kinesisStreamingBuffer.triggerAsyncFill();

        MessiMessage messiMessage = kinesisStreamingBuffer.poll(timeout, timeUnit);

        return messiMessage;
    }

    @Override
    public CompletableFuture<? extends MessiMessage> receiveAsync() {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                return receive(5, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public MessiCursor currentPosition() {
        throw new UnsupportedOperationException("TODO"); // TODO
    }

    @Override
    public void seek(long timestamp) {
        if (closed.get()) {
            throw new MessiClosedException();
        }

        kinesisConsumerBufferRef.get().seek(timestamp);
    }


    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closed.set(true);
        kinesisConsumerBufferRef.get().close();
    }
}
