package no.cantara.messi.kinesis;

public interface ProgressTracker {

    void registerProgress(String sequenceNumber);

    void registerTimePassed();

    /**
     * @return the last safe sequence-number that has been recorded to this cursor. Typically, a safe sequence-number is
     * one that will survive even a crash and restart of the running process.
     */
    String getSafeSequenceNumber();
}
