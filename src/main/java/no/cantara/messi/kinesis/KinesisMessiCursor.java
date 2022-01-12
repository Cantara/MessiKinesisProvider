package no.cantara.messi.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiCursorStartingPointType;
import no.cantara.messi.api.MessiNotCompatibleCursorException;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

class KinesisMessiCursor implements MessiCursor {

    private static final ObjectMapper mapper = new ObjectMapper();

    String shardId;
    MessiCursorStartingPointType type;
    Instant timestamp;
    String sequenceNumber;

    /**
     * Need not exactly match an existing ulid-value.
     */
    ULID.Value ulid;

    String externalId;
    Instant externalIdTimestamp;
    Duration externalIdTimestampTolerance;

    /**
     * Whether or not to include the element with ulid-value matching the lower-bound exactly.
     */
    boolean inclusive;

    KinesisMessiCursor(String shardId,
                       MessiCursorStartingPointType type,
                       Instant timestamp,
                       String sequenceNumber,
                       ULID.Value ulid,
                       String externalId,
                       Instant externalIdTimestamp,
                       Duration externalIdTimestampTolerance,
                       boolean inclusive) {
        this.shardId = shardId;
        this.type = type;
        this.timestamp = timestamp;
        this.sequenceNumber = sequenceNumber;
        this.ulid = ulid;
        this.externalId = externalId;
        this.externalIdTimestamp = externalIdTimestamp;
        this.externalIdTimestampTolerance = externalIdTimestampTolerance;
        this.inclusive = inclusive;
    }

    @Override
    public String checkpoint() {
        if (type != MessiCursorStartingPointType.AT_PROVIDER_SEQUENCE) {
            throw new IllegalStateException("Unable to produce checkpoint of cursor, please use cursor obtained from consumer and message");
        }
        ObjectNode node = mapper.createObjectNode();
        node.put("shardId", shardId);
        node.put("sequenceNumber", sequenceNumber);
        node.put("inclusive", inclusive);
        return node.toString();
    }

    @Override
    public int compareTo(MessiCursor _o) throws NullPointerException, MessiNotCompatibleCursorException {
        Objects.requireNonNull(_o);
        if (!getClass().equals(_o.getClass())) {
            throw new MessiNotCompatibleCursorException(String.format("Cursor classes are not compatible. this.getClass(): %s, other.getClass(): %s", getClass(), _o.getClass()));
        }
        if (type != MessiCursorStartingPointType.AT_PROVIDER_SEQUENCE || sequenceNumber == null || shardId == null) {
            throw new MessiNotCompatibleCursorException(String.format("This cursor must have this.type=%s to be compared and this.sequenceNumber must be non-null and this.shardId must be non-null.", MessiCursorStartingPointType.AT_PROVIDER_SEQUENCE));
        }
        KinesisMessiCursor o = (KinesisMessiCursor) _o;
        if (o.type != MessiCursorStartingPointType.AT_PROVIDER_SEQUENCE || o.sequenceNumber == null || o.shardId == null) {
            throw new MessiNotCompatibleCursorException(String.format("Other cursor must have other.type=%s to be compared and other.sequenceNumber must be non-null and other.shardId must be non-null.", MessiCursorStartingPointType.AT_PROVIDER_SEQUENCE));
        }
        if (!shardId.equals(o.shardId)) {
            throw new MessiNotCompatibleCursorException(String.format("shardId of this cursor and other cursor does not match. this.shardId: %s, other.shardId: %s", shardId, o.shardId));
        }
        int comparison = sequenceNumber.compareTo(o.sequenceNumber);
        if (comparison != 0) {
            return comparison;
        }
        if (inclusive == o.inclusive) {
            return 0;
        }
        if (inclusive) {
            return -1;
        } else {
            return 1;
        }
    }

    static class Builder implements MessiCursor.Builder {

        String shardId;
        MessiCursorStartingPointType type;
        Instant timestamp;
        String sequenceNumber;
        ULID.Value ulid;
        String externalId;
        Instant externalIdTimestamp;
        Duration externalIdTimestampTolerance;
        boolean inclusive = false;

        @Override
        public Builder shardId(String shardId) {
            this.shardId = shardId;
            return this;
        }

        @Override
        public Builder now() {
            this.type = MessiCursorStartingPointType.NOW;
            return this;
        }

        @Override
        public Builder oldest() {
            this.type = MessiCursorStartingPointType.OLDEST_RETAINED;
            return this;
        }

        @Override
        public Builder providerTimestamp(Instant timestamp) {
            this.type = MessiCursorStartingPointType.AT_PROVIDER_TIME;
            this.timestamp = timestamp;
            return this;
        }

        @Override
        public Builder providerSequenceNumber(String sequenceNumber) {
            this.type = MessiCursorStartingPointType.AT_PROVIDER_SEQUENCE;
            this.sequenceNumber = sequenceNumber;
            return this;
        }

        @Override
        public Builder ulid(ULID.Value ulid) {
            this.type = MessiCursorStartingPointType.AT_ULID;
            this.ulid = ulid;
            return this;
        }

        @Override
        public Builder externalId(String externalId, Instant externalIdTimestamp, Duration externalIdTimestampTolerance) {
            this.type = MessiCursorStartingPointType.AT_EXTERNAL_ID;
            this.externalId = externalId;
            this.externalIdTimestamp = externalIdTimestamp;
            this.externalIdTimestampTolerance = externalIdTimestampTolerance;
            return this;
        }

        @Override
        public Builder inclusive(boolean inclusive) {
            this.inclusive = inclusive;
            return this;
        }

        @Override
        public Builder checkpoint(String checkpoint) {
            this.type = MessiCursorStartingPointType.AT_PROVIDER_SEQUENCE;
            try {
                ObjectNode node = (ObjectNode) mapper.readTree(checkpoint);
                this.shardId = node.get("shardId").textValue();
                this.sequenceNumber = node.get("sequenceNumber").textValue();
                this.inclusive = node.get("inclusive").booleanValue();
                return this;
            } catch (RuntimeException | JsonProcessingException e) {
                throw new IllegalArgumentException("checkpoint is not valid", e);
            }
        }

        @Override
        public KinesisMessiCursor build() {
            Objects.requireNonNull(type);
            return new KinesisMessiCursor(shardId, type, timestamp, sequenceNumber, ulid, externalId, externalIdTimestamp, externalIdTimestampTolerance, inclusive);
        }
    }
}
