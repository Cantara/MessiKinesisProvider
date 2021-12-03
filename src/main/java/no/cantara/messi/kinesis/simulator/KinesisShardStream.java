package no.cantara.messi.kinesis.simulator;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.EncryptionType;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

class KinesisShardStream {

    final String shardId;
    final List<Record> records = new ArrayList<>(1000);

    KinesisShardStream(String shardId) {
        this.shardId = shardId;
    }

    String add(SdkBytes data, String partitionKey) {
        synchronized (records) {
            String sequenceNumber = String.valueOf(records.size());
            records.add(Record.builder()
                    .data(data)
                    .encryptionType(EncryptionType.NONE)
                    .partitionKey(partitionKey)
                    .approximateArrivalTimestamp(Instant.now())
                    .sequenceNumber(sequenceNumber)
                    .build());
            return sequenceNumber;
        }
    }

    String trimHorizonSequenceNumber() {
        return "0";
    }

    String nextSequenceNumber() {
        synchronized (records) {
            return String.valueOf(records.size());
        }
    }

    String sequenceAtTimestamp(Instant timestamp) {
        synchronized (records) {
            for (int i = 0; i < records.size(); i++) {
                Record record = records.get(i);
                if (!record.approximateArrivalTimestamp().isBefore(timestamp)) {
                    return record.sequenceNumber();
                }
            }
            return String.valueOf(records.size());
        }
    }

    public String sequenceNumberAfter(String sequenceNumber) {
        return String.valueOf(Integer.parseInt(sequenceNumber) + 1);
    }

    public List<Record> getRecords(String sequenceNumber, int limit) {
        int index = Integer.parseInt(sequenceNumber);
        List<Record> result = new ArrayList<>(limit);
        synchronized (records) {
            for (int i = 0; i < limit && index + i < records.size(); i++) {
                Record record = records.get(index + i);
                result.add(record);
            }
        }
        return result;
    }

    public int messagesBehindCurrent(String sequenceNumber) {
        int index = Integer.parseInt(sequenceNumber);
        int behind = records.size() - index - 1;
        return behind;
    }
}
