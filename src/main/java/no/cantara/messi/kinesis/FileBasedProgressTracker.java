package no.cantara.messi.kinesis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicReference;

public class FileBasedProgressTracker implements ProgressTracker {

    private static Logger log = LoggerFactory.getLogger(FileBasedProgressTracker.class);

    static class SequenceNumberRegistration {
        final Instant recorded;
        final String sequenceNumber;

        @JsonCreator(
                mode = JsonCreator.Mode.PROPERTIES
        )
        SequenceNumberRegistration(@JsonProperty("timestamp") Instant recorded, @JsonProperty("sequence") String sequenceNumber) {
            this.recorded = recorded;
            this.sequenceNumber = sequenceNumber;
        }

        @JsonProperty("timestamp")
        public Instant getRecorded() {
            return recorded;
        }

        @JsonProperty("sequence")
        public String getSequenceNumber() {
            return sequenceNumber;
        }
    }

    final ObjectMapper mapper;

    final Path checkpointPath;
    final String regionId;
    final String streamName;
    final String shardId;
    final AtomicReference<SequenceNumberRegistration> checkpointRef = new AtomicReference<>();
    final AtomicReference<SequenceNumberRegistration> lastRef = new AtomicReference<>();

    public FileBasedProgressTracker(Path checkpointPath, String regionId, String streamName, String shardId) {
        this.checkpointPath = checkpointPath;
        this.regionId = regionId;
        this.streamName = streamName;
        this.shardId = shardId;
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.setDateFormat(new StdDateFormat().withColonInTimeZone(true));
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configOverride(SequenceNumberRegistration.class).setInclude(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL));
        this.mapper = mapper;
        if (Files.exists(checkpointPath)) {
            if (!Files.isReadable(checkpointPath)) {
                throw new RuntimeException(String.format("File exists but is not readable by the jvm, unable to read checkpoint. File: '%s'", checkpointPath));
            }
            if (!Files.isWritable(checkpointPath)) {
                throw new RuntimeException(String.format("File is not writable by the jvm, unable to write checkpoints. File: '%s'", checkpointPath));
            }
            SequenceNumberRegistration checkpoint = readCheckpointFromFile(checkpointPath, mapper);
            checkpointRef.set(checkpoint);
        }
        if (checkpointRef.get() == null) {
            // dummy checkpoint to avoid NPE and allow algorithm a timestamp reference point to compare with
            checkpointRef.set(new SequenceNumberRegistration(Instant.now(), null));
        }
    }

    @Override
    public void registerProgress(String sequenceNumber) {
        Instant now = Instant.now();
        SequenceNumberRegistration currentRegistration = new SequenceNumberRegistration(now, sequenceNumber);
        lastRef.set(currentRegistration);
        SequenceNumberRegistration checkpoint = checkpointRef.get();
        if (checkpoint.recorded.plus(5, ChronoUnit.SECONDS).isBefore(now)) {
            writeCheckpointToDisk(currentRegistration);
            checkpointRef.set(currentRegistration);
        }
    }

    @Override
    public void registerTimePassed() {
        SequenceNumberRegistration currentRegistration = lastRef.get();
        if (currentRegistration == null) {
            return;
        }
        SequenceNumberRegistration checkpoint = checkpointRef.get();
        if (checkpoint == null) {
            return;
        }
        if (checkpoint.sequenceNumber != null) {
            if (currentRegistration.getSequenceNumber().equals(checkpoint.sequenceNumber)) {
                return; // checkpoint is already at latest sequence number
            }
        }
        Instant now = Instant.now();
        if (checkpoint.recorded.plus(5, ChronoUnit.SECONDS).isBefore(now)) {
            // record latest progress as checkpoint
            writeCheckpointToDisk(currentRegistration);
            checkpointRef.set(currentRegistration);
        }
    }

    @Override
    public String getSafeSequenceNumber() {
        return checkpointRef.get().sequenceNumber;
    }

    static SequenceNumberRegistration readCheckpointFromFile(Path checkpointPath, ObjectMapper mapper) {
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(checkpointPath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        SequenceNumberRegistration checkpoint;
        try {
            checkpoint = mapper.readValue(bytes, SequenceNumberRegistration.class);
            return checkpoint;
        } catch (IOException e) {
            log.warn("Exception while deserializing json file to SequenceNumberRegistration.class. Will start reading Kinesis stream from TRIM_HORIZON. File: '{}'", checkpointPath);
            return null;
        }
    }

    private void writeCheckpointToDisk(SequenceNumberRegistration newCheckpoint) {
        byte[] bytes;
        try {
            bytes = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(newCheckpoint);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        try {
            Files.write(checkpointPath, bytes, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
