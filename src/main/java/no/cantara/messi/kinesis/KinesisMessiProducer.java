package no.cantara.messi.kinesis;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiClosedException;
import no.cantara.messi.api.MessiProducer;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;
import no.cantara.messi.protos.MessiUlid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class KinesisMessiProducer implements MessiProducer {

    private static Logger log = LoggerFactory.getLogger(KinesisMessiProducer.class);

    private final AtomicBoolean closed = new AtomicBoolean();
    private final KinesisAsyncClient kinesisAsyncClient;
    private final String streamName;
    private final String topic;
    private final ULID ulid = new ULID();
    private final AtomicReference<ULID.Value> prevUlid = new AtomicReference<>(ulid.nextValue());

    public KinesisMessiProducer(KinesisAsyncClient kinesisAsyncClient, String streamName, String topic) {
        this.kinesisAsyncClient = kinesisAsyncClient;
        this.streamName = streamName;
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public void publish(MessiMessage... messiMessages) throws MessiClosedException {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        if (messiMessages == null || messiMessages.length == 0) {
            return;
        }

        List<MessiMessage> remainingMessages = new ArrayList<>();
        for (int i = 0; i < messiMessages.length; i++) {
            remainingMessages.add(messiMessages[i]);
        }

        for (int x = 0; remainingMessages.size() > 0; x++) {
            List<PutRecordsRequestEntry> records = new ArrayList<>(messiMessages.length);
            for (int i = 0; i < remainingMessages.size(); i++) {
                MessiMessage messiMessage = remainingMessages.get(i);
                if (!messiMessage.hasPartitionKey()) {
                    throw new IllegalArgumentException(String.format("Message at index %d is missing partition-key.", i));
                }

                ULID.Value ulid;
                if (messiMessage.hasUlid()) {
                    ulid = new ULID.Value(messiMessage.getUlid().getMsb(), messiMessage.getUlid().getLsb());
                } else {
                    ulid = MessiULIDUtils.nextMonotonicUlid(this.ulid, prevUlid.get());
                    messiMessage = messiMessage.toBuilder()
                            .setUlid(MessiUlid.newBuilder()
                                    .setMsb(ulid.getMostSignificantBits())
                                    .setLsb(ulid.getLeastSignificantBits())
                                    .build())
                            .build();
                }
                prevUlid.set(ulid);

                String partitionKey = messiMessage.getPartitionKey();
                byte[] messageBytes = messiMessage.toByteArray();
                SdkBytes sdkBytes = SdkBytes.fromByteArrayUnsafe(messageBytes);
                records.add(PutRecordsRequestEntry.builder()
                        .partitionKey(partitionKey)
                        .data(sdkBytes)
                        .build());
            }
            PutRecordsRequest request = PutRecordsRequest.builder()
                    .streamName(streamName)
                    .records(records)
                    .build();

            PutRecordsResponse response = kinesisAsyncClient.putRecords(request).join(); // send message-batch to Kinesis
            if (!response.sdkHttpResponse().isSuccessful()) {
                throw new RuntimeException(String.format("While putting records to Kinesis stream '%s'. statusCode=%s, text: '%s'",
                        streamName,
                        response.sdkHttpResponse().statusCode(),
                        response.sdkHttpResponse().statusText().orElse("")));
            }

            if (response.failedRecordCount() > 0) {

                /*
                 * At least one message failure
                 */

                List<PutRecordsResultEntry> recordResults = response.records();
                List<MessiMessage> failedMessages = new ArrayList<>(response.failedRecordCount());
                int successCount = recordResults.size() - response.failedRecordCount();
                log.warn("Failed to write all records to kinesis, potentially re-ordering messages in batch. successCount={}, failedCount={}", successCount, response.failedRecordCount());

                for (int j = 0; j < remainingMessages.size(); j++) {
                    MessiMessage message = remainingMessages.get(j);
                    PutRecordsResultEntry putRecordsResultEntry = recordResults.get(j);
                    String errorCode = putRecordsResultEntry.errorCode();

                    if (errorCode == null) {
                        // success - message written to Kinesis

                        if (log.isTraceEnabled()) {
                            String shardId = putRecordsResultEntry.shardId();
                            String sequenceNumber = putRecordsResultEntry.sequenceNumber();
                            log.trace("Message with ulid={} sent to shardId={} with sequenceNumber={}", MessiULIDUtils.toUlid(message.getUlid()), shardId, sequenceNumber);
                        }

                    } else {
                        // failure - failed to write message to Kinesis

                        failedMessages.add(message);

                        if (log.isTraceEnabled()) {
                            String errorMessage = putRecordsResultEntry.errorMessage();
                            log.trace("Message with ulid={} write to Kinesis failed. errorCode={}, errorMessage: {}", MessiULIDUtils.toUlid(message.getUlid()), errorCode, errorMessage);
                        }
                    }
                }

                remainingMessages = failedMessages;

            } else {

                /*
                 * All messages were successfully written to Kinesis.
                 */

                if (log.isTraceEnabled()) {
                    List<PutRecordsResultEntry> recordResults = response.records();
                    for (int j = 0; j < remainingMessages.size(); j++) {
                        MessiMessage message = remainingMessages.get(j);
                        PutRecordsResultEntry putRecordsResultEntry = recordResults.get(j);
                        String shardId = putRecordsResultEntry.shardId();
                        String sequenceNumber = putRecordsResultEntry.sequenceNumber();
                        log.trace("Message with ulid={} sent to shardId={} with sequenceNumber={}", MessiULIDUtils.toUlid(message.getUlid()), shardId, sequenceNumber);
                    }
                }

                remainingMessages.clear();
            }
        }
    }

    @Override
    public CompletableFuture<Void> publishAsync(MessiMessage... messiMessages) {
        if (closed.get()) {
            throw new MessiClosedException();
        }
        return CompletableFuture.supplyAsync(() -> {
            publish(messiMessages);
            return null;
        });
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closed.set(true);
    }
}
