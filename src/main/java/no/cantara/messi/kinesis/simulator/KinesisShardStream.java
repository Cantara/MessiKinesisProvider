package no.cantara.messi.kinesis.simulator;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.EncryptionType;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

class KinesisShardStream {

    final String shardId;
    final List<Record> records = new ArrayList<>(10000);

    KinesisShardStream(String shardId) {
        this.shardId = shardId;
    }

    String toSequenceNumber(int index) {
        String sequenceNumber = String.format("%012d", index);
        return sequenceNumber;
    }

    int toIndex(String sequenceNumber) {
        String sequenceNumberWithoutLeadingZeros = trimLeadingZeros(sequenceNumber);
        int index = Integer.parseInt(sequenceNumberWithoutLeadingZeros);
        return index;
    }

    String trimLeadingZeros(String sequenceNumber) {
        for (int i = 0; i < sequenceNumber.length(); i++) {
            char c = sequenceNumber.charAt(i);
            if (c != '0') {
                String sequenceNumberWithoutLeadingZeros = sequenceNumber.substring(i);
                return sequenceNumberWithoutLeadingZeros;
            }
        }
        return "0";
    }

    String add(SdkBytes data, String partitionKey) {
        synchronized (records) {
            String sequenceNumber = nextSequenceNumber();
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
            return toSequenceNumber(records.size());
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
            return nextSequenceNumber();
        }
    }

    public String sequenceNumberAfter(String sequenceNumber) {
        int index = toIndex(sequenceNumber);
        return toSequenceNumber(index + 1);
    }

    public List<Record> getRecords(String sequenceNumber, int limit) {
        int index = toIndex(sequenceNumber);
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
        int index = toIndex(sequenceNumber);
        int behind = records.size() - index - 1;
        return behind;
    }
}
