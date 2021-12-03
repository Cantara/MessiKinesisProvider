package no.cantara.messi.kinesis.simulator;

import no.cantara.messi.kinesis.KinesisMessiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.CreateStreamResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.EncryptionType;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class KinesisAsyncClientSimulator implements KinesisAsyncClient {

    private static Logger log = LoggerFactory.getLogger(KinesisMessiClient.class);

    final AtomicInteger nextShardId = new AtomicInteger(1);
    final Map<String, List<Shard>> shardsByStreamName = new ConcurrentHashMap<>();
    final Map<String, KinesisShardStream> streamByShardId = new ConcurrentHashMap<>();

    public KinesisAsyncClientSimulator(String streamName) {
        shardsByStreamName.computeIfAbsent(streamName, stream -> {
            Shard shard = Shard.builder()
                    .shardId(String.valueOf(nextShardId.getAndIncrement()))
                    .build();
            streamByShardId.computeIfAbsent(shard.shardId(), KinesisShardStream::new);
            List<Shard> result = new ArrayList<>(1);
            result.add(shard);
            return result;
        });
    }

    @Override
    public String serviceName() {
        return KinesisAsyncClient.SERVICE_NAME;
    }

    @Override
    public void close() {
    }

    @Override
    public CompletableFuture<ListStreamsResponse> listStreams(ListStreamsRequest listStreamsRequest) {
        ListStreamsResponse.Builder responseBuilder = ListStreamsResponse.builder();
        responseBuilder.sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build());
        return CompletableFuture.completedFuture(responseBuilder
                .streamNames(new ArrayList<>(shardsByStreamName.keySet()))
                .hasMoreStreams(false)
                .build());
    }

    @Override
    public CompletableFuture<CreateStreamResponse> createStream(CreateStreamRequest createStreamRequest) {
        if (createStreamRequest.shardCount() != 1) {
            throw new IllegalArgumentException("only shard-count of 1 is supported");
        }
        shardsByStreamName.computeIfAbsent(createStreamRequest.streamName(), streamName -> {
            Shard shard = Shard.builder()
                    .shardId(String.valueOf(nextShardId.getAndIncrement()))
                    .build();
            streamByShardId.computeIfAbsent(shard.shardId(), KinesisShardStream::new);
            List<Shard> result = new ArrayList<>(1);
            result.add(shard);
            return result;
        });
        CreateStreamResponse.Builder builder = CreateStreamResponse.builder();
        builder.sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build());
        return CompletableFuture.completedFuture(builder
                .build());
    }

    @Override
    public CompletableFuture<DescribeStreamResponse> describeStream(DescribeStreamRequest describeStreamRequest) {
        String streamName = describeStreamRequest.streamName();
        List<Shard> shards = shardsByStreamName.get(streamName);
        if (shards == null) {
            throw new IllegalArgumentException("Stream does not exist: " + streamName);
        }
        if (shards.isEmpty()) {
            throw new IllegalStateException("Stream does not contain any shards: " + streamName);
        }
        if (shards.size() > 1) {
            throw new IllegalStateException("More than one shard in stream.");
        }
        DescribeStreamResponse.Builder responseBuilder = DescribeStreamResponse.builder();
        responseBuilder.sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build());
        return CompletableFuture.completedFuture(responseBuilder
                .streamDescription(StreamDescription.builder()
                        .streamStatus(StreamStatus.ACTIVE)
                        .streamName(streamName)
                        .shards(Shard.builder()
                                .shardId(shards.get(0).shardId())
                                .build())
                        .build())
                .build());
    }

    @Override
    public CompletableFuture<GetShardIteratorResponse> getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
        List<Shard> shards = shardsByStreamName.get(getShardIteratorRequest.streamName());
        String shardId = getShardIteratorRequest.shardId();
        if (shards.stream().noneMatch(shard -> shard.shardId().equals(shardId))) {
            throw new IllegalArgumentException("Requested stream does not contain shardId: '" + shardId + "'");
        }
        KinesisShardStream kinesisStream = streamByShardId.get(shardId);
        if (kinesisStream == null) {
            throw new IllegalArgumentException("streamName does not exist");
        }
        String startingSequenceNumber = null;
        switch (getShardIteratorRequest.shardIteratorType()) {
            case AT_SEQUENCE_NUMBER:
                startingSequenceNumber = getShardIteratorRequest.startingSequenceNumber();
                break;
            case AFTER_SEQUENCE_NUMBER:
                startingSequenceNumber = String.valueOf(Long.parseLong(getShardIteratorRequest.startingSequenceNumber()) + 1L);
                break;
            case TRIM_HORIZON:
                startingSequenceNumber = kinesisStream.trimHorizonSequenceNumber();
                break;
            case LATEST:
                startingSequenceNumber = kinesisStream.nextSequenceNumber();
                break;
            case AT_TIMESTAMP:
                startingSequenceNumber = kinesisStream.sequenceAtTimestamp(getShardIteratorRequest.timestamp());
                break;
            case UNKNOWN_TO_SDK_VERSION:
                throw new IllegalArgumentException("shardIteratorType unknown");
        }
        GetShardIteratorResponse.Builder responseBuilder = GetShardIteratorResponse.builder();
        responseBuilder.sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build());
        return CompletableFuture.completedFuture(responseBuilder
                .shardIterator(toShardIterator(shardId, startingSequenceNumber))
                .build());
    }

    @Override
    public CompletableFuture<GetRecordsResponse> getRecords(GetRecordsRequest getRecordsRequest) {
        if (getRecordsRequest.limit() < 1 || 10000 < getRecordsRequest.limit()) {
            throw new IllegalArgumentException("limit must be between 1 and 10000");
        }
        String[] parts = getRecordsRequest.shardIterator().split(";");
        if (parts.length != 2) {
            throw new IllegalArgumentException("invalid shardIterator format");
        }
        String shardId = parts[0];
        String sequenceNumber = parts[1];
        KinesisShardStream kinesisStream = streamByShardId.get(shardId);
        if (kinesisStream == null) {
            throw new IllegalArgumentException("Kinesis shard does not exists: '" + shardId + "'");
        }
        List<Record> records = kinesisStream.getRecords(sequenceNumber, getRecordsRequest.limit());
        GetRecordsResponse.Builder responseBuilder = GetRecordsResponse.builder();
        responseBuilder.sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build());
        String nextSequenceAfter;
        if (records.isEmpty()) {
            nextSequenceAfter = sequenceNumber;
        } else {
            nextSequenceAfter = kinesisStream.sequenceNumberAfter(records.get(records.size() - 1).sequenceNumber());
        }
        String nextShardIterator = toShardIterator(shardId, nextSequenceAfter);
        return CompletableFuture.completedFuture(responseBuilder
                .records(records)
                .nextShardIterator(nextShardIterator)
                .millisBehindLatest(1000L * kinesisStream.messagesBehindCurrent(nextSequenceAfter))
                .build());
    }

    String toShardIterator(String shardId, String sequenceNumber) {
        Objects.requireNonNull(shardId);
        Objects.requireNonNull(sequenceNumber);
        return shardId + ";" + sequenceNumber;
    }

    @Override
    public CompletableFuture<PutRecordsResponse> putRecords(PutRecordsRequest putRecordsRequest) {
        List<Shard> shards = shardsByStreamName.get(putRecordsRequest.streamName());
        if (shards == null) {
            throw new IllegalArgumentException("streamName does not exist: '" + putRecordsRequest.streamName() + "'");
        }
        if (shards.isEmpty()) {
            throw new IllegalArgumentException("streamName does not contain any shards: '" + putRecordsRequest.streamName() + "'");
        }
        if (shards.size() > 1) {
            throw new IllegalStateException("More than one shard in stream.");
        }
        String shardId = shards.get(0).shardId();
        KinesisShardStream kinesisStream = streamByShardId.get(shardId);
        if (kinesisStream == null) {
            throw new IllegalArgumentException("Stream does not exist: streamName: '" + putRecordsRequest.streamName() + "', shardId: '" + shardId + "'");
        }
        List<PutRecordsResultEntry> responseRecordEntries = new ArrayList<>();
        for (PutRecordsRequestEntry record : putRecordsRequest.records()) {
            String sequenceNumber = kinesisStream.add(record.data(), record.partitionKey());
            responseRecordEntries.add(PutRecordsResultEntry.builder()
                    .shardId(shardId)
                    .sequenceNumber(sequenceNumber)
                    .build());
            // TODO simulate full and partial errors
        }
        PutRecordsResponse.Builder responseBuilder = PutRecordsResponse.builder();
        responseBuilder.sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build());
        return CompletableFuture.completedFuture(responseBuilder
                .encryptionType(EncryptionType.NONE)
                .failedRecordCount(0)
                .records(responseRecordEntries)
                .build());
    }
}
