package no.cantara.messi.kinesis;

import no.cantara.config.ApplicationProperties;
import no.cantara.messi.api.MessiClientFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class KinesisMessiClientFactory implements MessiClientFactory {

    @Override
    public Class<?> providerClass() {
        return KinesisMessiClient.class;
    }

    @Override
    public String alias() {
        return "kinesis";
    }

    @Override
    public KinesisMessiClient create(ApplicationProperties configuration) {

        String region = configuration.get("region");
        if (region == null) {
            throw new IllegalArgumentException("Missing configuration property: region");
        }
        String accessKeyId = configuration.get("credentials.keyid");
        if (accessKeyId == null) {
            throw new IllegalArgumentException("Missing configuration property: credentials.keyid");
        }
        String secretAccessKey = configuration.get("credentials.secret");
        if (secretAccessKey == null) {
            throw new IllegalArgumentException("Missing configuration property: credentials.secret");
        }
        String streamName = configuration.get("stream.name");
        if (streamName == null) {
            throw new IllegalArgumentException("Missing configuration property: stream.name");
        }
        String streamShardCount = configuration.get("stream.shardcount");
        if (streamShardCount == null) {
            throw new IllegalArgumentException("Missing configuration property: stream.shardcount");
        }
        int shardCount = Integer.parseInt(streamShardCount);

        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
        KinesisAsyncClient kinesisAsyncClient = KinesisAsyncClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .region(Region.of(region))
                .build();

        List<String> streamNames = KinesisUtils.listStreamNames(kinesisAsyncClient);
        if (!streamNames.contains(streamName)) {
            KinesisUtils.createStream(kinesisAsyncClient, streamName, shardCount);
        }

        Path kinesisCheckpointsFolder = Paths.get(configuration.get("checkpoints.folder"));

        return new KinesisMessiClient(kinesisAsyncClient, streamName, kinesisCheckpointsFolder);
    }
}
