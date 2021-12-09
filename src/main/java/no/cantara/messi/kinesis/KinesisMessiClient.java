package no.cantara.messi.kinesis;

import no.cantara.messi.api.MessiClient;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiMetadataClient;
import no.cantara.messi.api.MessiTopic;
import no.cantara.messi.protos.MessiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class KinesisMessiClient implements MessiClient {

    private static Logger log = LoggerFactory.getLogger(KinesisMessiClient.class);

    final KinesisAsyncClient kinesisAsyncClient;
    final String streamName;
    final Path kinesisCheckpointsFolder;

    final AtomicBoolean closed = new AtomicBoolean();

    final Map<String, KinesisMessiTopic> topicByName = new ConcurrentHashMap<>();

    public KinesisMessiClient(KinesisAsyncClient kinesisAsyncClient, String streamName, Path kinesisCheckpointsFolder) {
        this.kinesisAsyncClient = kinesisAsyncClient;
        this.streamName = streamName;
        this.kinesisCheckpointsFolder = kinesisCheckpointsFolder;
    }

    @Override
    public MessiTopic topicOf(String name) {
        return topicByName.computeIfAbsent(name, topicName -> new KinesisMessiTopic(topicName, kinesisAsyncClient, streamName));
    }

    @Override
    public MessiCursor.Builder cursorOf() {
        return new KinesisMessiCursor.Builder();
    }

    @Override
    public MessiMessage lastMessage(String topic, String shardId) {
        throw new UnsupportedOperationException("Kinesis does not suppor reading the last-message in stream, use time-based or sequence-based cursor instead");
    }

    @Override
    public List<String> shards(String topic) {
        MessiTopic messiTopic = topicOf(topic);
        return messiTopic.shards();
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closed.set(true);
        for (KinesisMessiTopic topic : topicByName.values()) {
            topic.close();
        }
        topicByName.clear();
        kinesisAsyncClient.close();
    }

    @Override
    public MessiMetadataClient metadata(String topic) {
        throw new UnsupportedOperationException("Kinesis provider does not support metadata-client");
    }
}
