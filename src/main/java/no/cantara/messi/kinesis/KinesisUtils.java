package no.cantara.messi.kinesis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.CreateStreamResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class KinesisUtils {

    private static final Logger log = LoggerFactory.getLogger(KinesisUtils.class);

    public static List<String> listStreamNames(KinesisAsyncClient kinesisAsyncClient) {
        List<String> allStreamNames = new ArrayList<>();
        String exclusiveStartStreamName = null;
        ListStreamsResponse listStreamsResponse;
        do {
            listStreamsResponse = kinesisAsyncClient.listStreams(ListStreamsRequest.builder()
                    .exclusiveStartStreamName(exclusiveStartStreamName)
                    .limit(20)
                    .build()).join();
            if (!listStreamsResponse.sdkHttpResponse().isSuccessful()) {
                throw new RuntimeException(String.format("Did not succeed in listing Kinesis streams. statusCode=%s, text: '%s'",
                        listStreamsResponse.sdkHttpResponse().statusCode(),
                        listStreamsResponse.sdkHttpResponse().statusText().orElse("")));
            }
            if (!listStreamsResponse.hasStreamNames()) {
                break;
            }
            List<String> streamNames = listStreamsResponse.streamNames();
            if (streamNames.isEmpty()) {
                break;
            }
            allStreamNames.addAll(streamNames);
            exclusiveStartStreamName = streamNames.get(streamNames.size() - 1);
        } while (listStreamsResponse.hasMoreStreams());
        return allStreamNames;
    }

    public static void createStream(KinesisAsyncClient kinesisAsyncClient, String streamName, int shardCount) {
        CreateStreamResponse createStreamResponse = kinesisAsyncClient.createStream(CreateStreamRequest.builder()
                .streamName(streamName)
                .shardCount(shardCount)
                .build()).join();
        SdkHttpResponse createStreamSdkHttpResponse = createStreamResponse.sdkHttpResponse();
        if (!createStreamSdkHttpResponse.isSuccessful()) {
            throw new RuntimeException(String.format("Unable to create Kinesis stream. statusCode=%s, statusText='%s'",
                    createStreamSdkHttpResponse.statusCode(), createStreamSdkHttpResponse.statusText().orElse("")));
        }
    }

    public static StreamDescription waitForStreamToBecomeAvailable(KinesisAsyncClient kinesisAsyncClient, String streamName) {
        log.info("Waiting for {} to become ACTIVE...", streamName);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.MINUTES.toMillis(10);
        while (System.currentTimeMillis() < endTime) {
            DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                    .streamName(streamName)
                    .limit(10)// ask for no more than 10 shards at a time -- this is an optional parameter
                    .build();
            DescribeStreamResponse describeStreamResponse = kinesisAsyncClient.describeStream(describeStreamRequest).join();
            if (!describeStreamResponse.sdkHttpResponse().isSuccessful()) {
                throw new RuntimeException(String.format("Did not succeed in describing Kinesis stream '%s'. statusCode=%s, text: '%s'",
                        streamName,
                        describeStreamResponse.sdkHttpResponse().statusCode(),
                        describeStreamResponse.sdkHttpResponse().statusText().orElse("")));
            }

            StreamStatus streamStatus = describeStreamResponse.streamDescription().streamStatus();
            log.info("- current state: {}", streamStatus.name());
            if (StreamStatus.ACTIVE.equals(streamStatus)) {
                return describeStreamResponse.streamDescription();
            }

            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(3));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        throw new RuntimeException(String.format("Stream %s never became active", streamName));
    }
}
