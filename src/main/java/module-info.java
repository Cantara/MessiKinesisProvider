module messi.provider.kinesis {
    requires messi.sdk;
    requires property.config;

    requires org.slf4j;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.datatype.jsr310;
    requires de.huxhorn.sulky.ulid;

    requires software.amazon.awssdk.auth;
    requires software.amazon.awssdk.core;
    requires software.amazon.awssdk.http;
    requires software.amazon.awssdk.regions;
    requires software.amazon.awssdk.services.kinesis;

    provides no.cantara.messi.api.MessiClientFactory with
            no.cantara.messi.kinesis.KinesisMessiClientFactory,
            no.cantara.messi.kinesis.simulator.KinesisSimulatorMessiClientFactory;

    exports no.cantara.messi.kinesis;
    exports no.cantara.messi.kinesis.simulator;
}