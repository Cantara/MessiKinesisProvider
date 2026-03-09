# MessiKinesisProvider

## Purpose
AWS Kinesis provider implementation for the Messi messaging and streaming abstraction layer. Enables applications using the Messi SDK to produce and consume messages via Amazon Kinesis Data Streams.

## Tech Stack
- Language: Java 11+
- Framework: None (library)
- Build: Maven
- Key dependencies: Messi SDK, AWS Kinesis SDK, Protocol Buffers

## Architecture
Provider library implementing the Messi SDK interfaces for Amazon Kinesis. Uses the provider pattern (Java SPI) so applications can switch between messaging backends (Kinesis, SQS, S3) by swapping the provider dependency without code changes. Messages are serialized using Protocol Buffers.

## Key Entry Points
- Messi Kinesis provider implementation classes
- `pom.xml` - Maven coordinates: `no.cantara.messi:messi-kinesis-provider`

## Development
```bash
# Build
mvn clean install

# Test
mvn test
```

## Domain Context
Cloud messaging infrastructure. Part of the Messi messaging abstraction ecosystem, providing AWS Kinesis as a streaming backend for event-driven architectures and real-time data pipelines.
