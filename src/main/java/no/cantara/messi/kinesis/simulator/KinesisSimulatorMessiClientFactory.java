package no.cantara.messi.kinesis.simulator;

import no.cantara.config.ApplicationProperties;
import no.cantara.messi.api.MessiClientFactory;
import no.cantara.messi.kinesis.KinesisMessiClient;
import no.cantara.messi.kinesis.KinesisUtils;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class KinesisSimulatorMessiClientFactory implements MessiClientFactory {

    @Override
    public Class<?> providerClass() {
        return KinesisMessiClient.class;
    }

    @Override
    public String alias() {
        return "kinesis-simulator";
    }

    @Override
    public KinesisMessiClient create(ApplicationProperties configuration) {
        String streamName = configuration.get("stream.name");
        if (streamName == null) {
            throw new IllegalArgumentException("Missing configuration property: stream.name");
        }
        String streamShardCount = configuration.get("stream.shardcount");
        if (streamShardCount == null) {
            throw new IllegalArgumentException("Missing configuration property: stream.shardcount");
        }
        int shardCount = Integer.parseInt(streamShardCount);

        KinesisAsyncClient kinesisAsyncClient = new KinesisAsyncClientSimulator(streamName);

        List<String> streamNames = KinesisUtils.listStreamNames(kinesisAsyncClient);
        if (!streamNames.contains(streamName)) {
            KinesisUtils.createStream(kinesisAsyncClient, streamName, shardCount);
        }

        Path kinesisCheckpointsFolder = Paths.get(configuration.get("checkpoints.folder"));

        return new KinesisMessiClient(kinesisAsyncClient, streamName, kinesisCheckpointsFolder);
    }
}
