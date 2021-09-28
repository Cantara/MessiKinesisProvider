package no.cantara.messi.kinesis;

import com.google.protobuf.ByteString;
import de.huxhorn.sulky.ulid.ULID;
import no.cantara.config.ApplicationProperties;
import no.cantara.config.ProviderLoader;
import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiClientFactory;
import no.cantara.messi.api.MessiConsumer;
import no.cantara.messi.api.MessiProducer;
import no.cantara.messi.api.MessiULIDUtils;
import no.cantara.messi.protos.MessiMessage;
import no.cantara.messi.protos.MessiOrdering;
import no.cantara.messi.protos.MessiProvider;
import no.cantara.messi.protos.MessiSource;
import no.cantara.messi.protos.MessiUlid;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class MessiClientTck {

    MessiClient client;

    @BeforeMethod
    public void createMessiClient() {
        ApplicationProperties applicationProperties = ApplicationProperties.builder()
                .testDefaults()
                .filesystemPropertiesFile("secret/credentials.properties")
                .build();
        client = ProviderLoader.configure(applicationProperties, "kinesis-simulator", MessiClientFactory.class);
    }

    @AfterMethod
    public void closeMessiClient() {
        client.close();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void thatLastMessageIsUnsupported() {
        assertNull(client.lastMessage("the-topic"));
    }

    @Test
    public void thatAllFieldsOfMessageSurvivesStream() throws Exception {
        ULID.Value ulid = new ULID().nextValue();
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.newBuilder()
                            .setPartitionKey("pk1")
                            .setUlid(MessiUlid.newBuilder().setMsb(ulid.getMostSignificantBits()).setLsb(ulid.getLeastSignificantBits()).build())
                            .setOrdering(MessiOrdering.newBuilder()
                                    .setGroup("og1")
                                    .setSequenceNumber(1)
                                    .build())
                            .setExternalId("a")
                            .putData("payload1", ByteString.copyFromUtf8("p1"))
                            .putData("payload2", ByteString.copyFromUtf8("p2"))
                            .build(),
                    MessiMessage.newBuilder()
                            .setPartitionKey("pk1")
                            .setOrdering(MessiOrdering.newBuilder()
                                    .setGroup("og1")
                                    .setSequenceNumber(2)
                                    .build())
                            .setExternalId("b")
                            .putData("payload1", ByteString.copyFromUtf8("p3"))
                            .putData("payload2", ByteString.copyFromUtf8("p4"))
                            .build(),
                    MessiMessage.newBuilder()
                            .setPartitionKey("pk1")
                            .setOrdering(MessiOrdering.newBuilder()
                                    .setGroup("og1")
                                    .setSequenceNumber(3)
                                    .build())
                            .setExternalId("c")
                            .putData("payload1", ByteString.copyFromUtf8("p5"))
                            .putData("payload2", ByteString.copyFromUtf8("p6"))
                            .setProvider(MessiProvider.newBuilder()
                                    .setPublishedTimestamp(123)
                                    .setShardId("shardId123")
                                    .setSequenceNumber("three")
                                    .build())
                            .setSource(MessiSource.newBuilder()
                                    .setClientSourceId("client-source-id-123")
                                    .build())
                            .putAttributes("key1", "value1")
                            .putAttributes("some-other-key", "some other value")
                            .putAttributes("iamanattribute", "yes I am")
                            .build()
            );
        }


        try (MessiConsumer consumer = client.consumer("the-topic", client.cursorOf()
                .shardId(client.shards("the-topic").get(0))
                .ulid(ulid)
                .inclusive(true)
                .build())) {
            {
                MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertEquals(MessiULIDUtils.toUlid(message.getUlid()), ulid);
                assertEquals(message.getPartitionKey(), "pk1");
                assertEquals(message.getOrdering().getGroup(), "og1");
                assertEquals(message.getOrdering().getSequenceNumber(), 1);
                assertEquals(message.getExternalId(), "a");
                assertEquals(message.getDataCount(), 2);
                assertEquals(message.getDataOrThrow("payload1"), ByteString.copyFromUtf8("p1"));
                assertEquals(message.getDataOrThrow("payload2"), ByteString.copyFromUtf8("p2"));
            }
            {
                MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertEquals(message.getPartitionKey(), "pk1");
                assertNotNull(message.getUlid());
                assertEquals(message.getOrdering().getGroup(), "og1");
                assertEquals(message.getOrdering().getSequenceNumber(), 2);
                assertEquals(message.getExternalId(), "b");
                assertEquals(message.getDataCount(), 2);
                assertEquals(message.getDataOrThrow("payload1"), ByteString.copyFromUtf8("p3"));
                assertEquals(message.getDataOrThrow("payload2"), ByteString.copyFromUtf8("p4"));
            }
            {
                MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertEquals(message.getPartitionKey(), "pk1");
                assertNotNull(message.getUlid());
                assertEquals(message.getOrdering().getGroup(), "og1");
                assertEquals(message.getOrdering().getSequenceNumber(), 3);
                assertEquals(message.getExternalId(), "c");
                assertEquals(message.getDataCount(), 2);
                assertEquals(message.getDataOrThrow("payload1"), ByteString.copyFromUtf8("p5"));
                assertEquals(message.getDataOrThrow("payload2"), ByteString.copyFromUtf8("p6"));
                assertTrue(message.hasProvider());
                assertNotNull(message.getProvider());
                assertNotEquals(message.getProvider().getPublishedTimestamp(), 123);
                assertNotNull(message.getProvider().getShardId());
                assertNotEquals(message.getProvider().getShardId(), "shardId123");
                assertNotNull(message.getProvider().getSequenceNumber());
                assertNotEquals(message.getProvider().getSequenceNumber(), "three");
                assertEquals(message.getSource().getClientSourceId(), "client-source-id-123");
                assertEquals(message.getAttributesCount(), 3);
                assertEquals(message.getAttributesOrThrow("key1"), "value1");
                assertEquals(message.getAttributesOrThrow("some-other-key"), "some other value");
                assertEquals(message.getAttributesOrThrow("iamanattribute"), "yes I am");
            }
        }
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        try (MessiConsumer consumer = client.consumer("the-topic")) {

            try (MessiProducer producer = client.producer("the-topic")) {
                producer.publish(MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build());
            }

            MessiMessage message = consumer.receive(5, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "a");
            assertEquals(message.getDataCount(), 2);
        }
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerAsynchronously() {
        try (MessiConsumer consumer = client.consumer("the-topic")) {

            CompletableFuture<? extends MessiMessage> future = consumer.receiveAsync();

            try (MessiProducer producer = client.producer("the-topic")) {
                producer.publish(MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build());
            }

            MessiMessage message = future.join();
            assertEquals(message.getExternalId(), "a");
            assertEquals(message.getDataCount(), 2);
        }
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        try (MessiConsumer consumer = client.consumer("the-topic")) {

            try (MessiProducer producer = client.producer("the-topic")) {
                producer.publish(
                        MessiMessage.newBuilder().setPartitionKey("pk1").setUlid(MessiULIDUtils.toMessiUlid(new ULID().nextValue())).setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build()
                );
            }

            MessiMessage message1 = consumer.receive(5, TimeUnit.SECONDS);
            MessiMessage message2 = consumer.receive(1, TimeUnit.SECONDS);
            MessiMessage message3 = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message1.getExternalId(), "a");
            assertEquals(message2.getExternalId(), "b");
            assertEquals(message3.getExternalId(), "c");
        }
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerAsynchronously() {
        try (MessiConsumer consumer = client.consumer("the-topic")) {

            CompletableFuture<List<MessiMessage>> future = receiveAsyncAddMessageAndRepeatRecursive(consumer, "c", new ArrayList<>());

            try (MessiProducer producer = client.producer("the-topic")) {
                producer.publish(
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build()
                );
            }

            List<MessiMessage> messages = future.join();

            assertEquals(messages.get(0).getExternalId(), "a");
            assertEquals(messages.get(1).getExternalId(), "b");
            assertEquals(messages.get(2).getExternalId(), "c");
        }
    }

    private CompletableFuture<List<MessiMessage>> receiveAsyncAddMessageAndRepeatRecursive(MessiConsumer consumer, String endExternalId, List<MessiMessage> messages) {
        return consumer.receiveAsync().thenCompose(message -> {
            messages.add(message);
            if (endExternalId.equals(message.getExternalId())) {
                return CompletableFuture.completedFuture(messages);
            }
            return receiveAsyncAddMessageAndRepeatRecursive(consumer, endExternalId, messages);
        });
    }

    @Test
    public void thatMessagesCanBeConsumedByMultipleConsumers() {
        try (MessiConsumer consumer1 = client.consumer("the-topic");
             MessiConsumer consumer2 = client.consumer("the-topic")) {

            CompletableFuture<List<MessiMessage>> future1 = receiveAsyncAddMessageAndRepeatRecursive(consumer1, "c", new ArrayList<>());
            CompletableFuture<List<MessiMessage>> future2 = receiveAsyncAddMessageAndRepeatRecursive(consumer2, "c", new ArrayList<>());

            try (MessiProducer producer = client.producer("the-topic")) {
                producer.publish(
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                        MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build()
                );
            }

            List<MessiMessage> messages1 = future1.join();
            assertEquals(messages1.get(0).getExternalId(), "a");
            assertEquals(messages1.get(1).getExternalId(), "b");
            assertEquals(messages1.get(2).getExternalId(), "c");

            List<MessiMessage> messages2 = future2.join();
            assertEquals(messages2.get(0).getExternalId(), "a");
            assertEquals(messages2.get(1).getExternalId(), "b");
            assertEquals(messages2.get(2).getExternalId(), "c");
        }
    }

    @Test
    public void thatConsumerCanReadFromBeginning() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic")) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "a");
        }
    }

    @Test
    public void thatConsumerCanReadFromFirstMessage() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic", client.cursorOf()
                .shardId(client.shards("the-topic").get(0))
                .externalId("a", Instant.now(), Duration.ofMinutes(1))
                .build())) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "b");
        }
    }

    @Test
    public void thatConsumerCanReadFromMiddle() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic", client.cursorOf()
                .shardId(client.shards("the-topic").get(0))
                .externalId("b", Instant.now(), Duration.ofMinutes(1))
                .build())) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "c");
        }
        try (MessiConsumer consumer = client.consumer("the-topic", client.cursorOf()
                .shardId(client.shards("the-topic").get(0))
                .externalId("c", Instant.now(), Duration.ofMinutes(1))
                .inclusive(true)
                .build())) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "c");
        }
    }

    @Test
    public void thatConsumerCanReadFromRightBeforeLast() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic", client.cursorOf()
                .shardId(client.shards("the-topic").get(0))
                .externalId("c", Instant.now(), Duration.ofMinutes(1))
                .build())) {
            MessiMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.getExternalId(), "d");
        }
    }

    @Test
    public void thatConsumerCanReadFromLast() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        try (MessiConsumer consumer = client.consumer("the-topic", client.cursorOf()
                .shardId(client.shards("the-topic").get(0))
                .externalId("d", Instant.now(), Duration.ofMinutes(1))
                .build())) {
            MessiMessage message = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(message);
        }
    }

    @Test
    public void thatCheckpointCanBeCreatedAndUsed() throws Exception {
        try (MessiProducer producer = client.producer("the-topic")) {
            producer.publish(
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build(),
                    MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build()
            );
        }
        String checkpointAt;
        String checkpointAfter;
        try (MessiConsumer consumer = client.consumer("the-topic")) {
            MessiMessage a = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(a);
            MessiMessage b = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(b);
            MessiMessage c = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(c);
            assertEquals(c.getExternalId(), "c");
            checkpointAt = consumer.cursorAt(c).checkpoint();
            checkpointAfter = consumer.cursorAfter(c).checkpoint();
            MessiMessage d = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(d);
            assertEquals(d.getExternalId(), "d");
            MessiMessage e = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(e);
        }
        try (MessiConsumer consumer = client.consumer("the-topic", client.cursorOf().checkpoint(checkpointAt).build())) {
            MessiMessage c = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(c);
            assertEquals(c.getExternalId(), "c");
            MessiMessage d = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(d);
            assertEquals(d.getExternalId(), "d");
            MessiMessage e = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(e);
        }
        try (MessiConsumer consumer = client.consumer("the-topic", client.cursorOf().checkpoint(checkpointAfter).build())) {
            MessiMessage d = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(d);
            assertEquals(d.getExternalId(), "d");
            MessiMessage e = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(e);
        }
    }

    @Test
    public void thatSeekToWorks() throws Exception {
        long timestampBeforeA;
        long timestampBeforeB;
        long timestampBeforeC;
        long timestampBeforeD;
        long timestampAfterD;
        try (MessiProducer producer = client.producer("the-topic")) {
            timestampBeforeA = System.currentTimeMillis();
            producer.publish(MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("a").putData("payload1", ByteString.copyFrom(new byte[5])).putData("payload2", ByteString.copyFrom(new byte[5])).build());
            Thread.sleep(5);
            timestampBeforeB = System.currentTimeMillis();
            producer.publish(MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("b").putData("payload1", ByteString.copyFrom(new byte[3])).putData("payload2", ByteString.copyFrom(new byte[3])).build());
            Thread.sleep(5);
            timestampBeforeC = System.currentTimeMillis();
            producer.publish(MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("c").putData("payload1", ByteString.copyFrom(new byte[7])).putData("payload2", ByteString.copyFrom(new byte[7])).build());
            Thread.sleep(5);
            timestampBeforeD = System.currentTimeMillis();
            producer.publish(MessiMessage.newBuilder().setPartitionKey("pk1").setExternalId("d").putData("payload1", ByteString.copyFrom(new byte[8])).putData("payload2", ByteString.copyFrom(new byte[8])).build());
            Thread.sleep(5);
            timestampAfterD = System.currentTimeMillis();
        }
        try (MessiConsumer consumer = client.consumer("the-topic")) {
            consumer.seek(timestampAfterD);
            assertNull(consumer.receive(100, TimeUnit.MILLISECONDS));
            consumer.seek(timestampBeforeD);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).getExternalId(), "d");
            consumer.seek(timestampBeforeB);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).getExternalId(), "b");
            consumer.seek(timestampBeforeC);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).getExternalId(), "c");
            consumer.seek(timestampBeforeA);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).getExternalId(), "a");
        }
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void thatMetadataIsUnsupported() {
        client.metadata("the-topic");
    }
}
