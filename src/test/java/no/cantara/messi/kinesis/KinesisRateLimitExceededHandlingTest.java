package no.cantara.messi.kinesis;

import com.google.protobuf.ByteString;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiProducer;
import no.cantara.messi.api.MessiShard;
import no.cantara.messi.api.MessiStreamingConsumer;
import no.cantara.messi.api.MessiTopic;
import no.cantara.messi.kinesis.simulator.KinesisAsyncClientSimulator;
import no.cantara.messi.protos.MessiMessage;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class KinesisRateLimitExceededHandlingTest {

    KinesisAsyncClientSimulator kinesisAsyncClient;
    MessiClient client;
    MessiTopic topic;
    MessiShard shard;

    @BeforeMethod
    public void createMessiClient() {
        kinesisAsyncClient = new KinesisAsyncClientSimulator("mystream");
        KinesisUtils.createStream(kinesisAsyncClient, "mystream", 1);
        Path kinesisCheckpointsFolder = Paths.get("target/kinesis-simulator-v45inoalfk");
        client = new KinesisMessiClient(kinesisAsyncClient, "mystream", kinesisCheckpointsFolder, 100);
        topic = client.topicOf("the-topic");
        shard = topic.shardOf(topic.firstShard());
        assertTrue(shard.supportsStreaming());
    }

    @AfterMethod
    public void closeMessiClient() {
        client.close();
    }

    private CompletableFuture<List<MessiMessage>> receiveAsyncAddMessageAndRepeatRecursive(MessiStreamingConsumer consumer, String endExternalId, List<MessiMessage> messages) {
        return consumer.receiveAsync().thenCompose(message -> {
            messages.add(message);
            if (endExternalId.equals(message.getExternalId())) {
                return CompletableFuture.completedFuture(messages);
            }
            return receiveAsyncAddMessageAndRepeatRecursive(consumer, endExternalId, messages);
        });
    }

    @Test
    public void thatMessagesCanBeConsumedByMultipleConsumers() throws InterruptedException {
        try (MessiProducer producer = topic.producer()) {

            try (MessiStreamingConsumer consumer = shard.streamingConsumer(shard.cursorAtTrimHorizon())) {

                producer.publish(
                        MessiMessage.newBuilder().setExternalId("a").setPartitionKey("pk1").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build()
                );

                MessiMessage message1 = consumer.receive(1, TimeUnit.MILLISECONDS);
                assertEquals(message1.getExternalId(), "a");

                producer.publish(
                        MessiMessage.newBuilder().setExternalId("b").setPartitionKey("pk1").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                        MessiMessage.newBuilder().setExternalId("c").setPartitionKey("pk1").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build()
                );

                kinesisAsyncClient.triggerRateLimitExceededOnNextGetRecordsCall();

                MessiMessage message2 = consumer.receive(1000, TimeUnit.MILLISECONDS);
                assertEquals(message2.getExternalId(), "b");

                MessiMessage message3 = consumer.receive(1, TimeUnit.MILLISECONDS);
                assertEquals(message3.getExternalId(), "c");
            }
        }
    }

}
