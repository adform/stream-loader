# Stream Loader ![](https://github.com/adform/stream-loader/workflows/build/badge.svg)

Stream loader is a collection of libraries providing means to load data from [Kafka](https://kafka.apache.org/) into arbitrary storages such as [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html), [S3](https://aws.amazon.com/s3/), [ClickHouse](https://clickhouse.tech/) or [Vertica](https://www.vertica.com/) using exactly-once semantics. Users can easily implement various highly customized loaders by combining out of the box components for record formatting and encoding, data storage, stream grouping and so on, implementing their own or extending the existing ones, if needed.

## Getting Started

Stream loader is currently available for Scala 2.13 only, to get started add the following to your `build.sbt`:

```scala
libraryDependencies += "com.adform" %% "stream-loader-core" % "0.1.4"
```

Various storage specific parts are split into separate artifacts to reduce dependencies and coupling, currently the following artifacts are available:

- `stream-loader-core`: core functionality _(required)_.
- `stream-loader-clickhouse`: ClickHouse storage and binary file format encoders.
- `stream-loader-hadoop`: HDFS storage, components for working with parquet files.
- `stream-loader-s3`: S3 compatible storage.
- `stream-loader-vertica`: Vertica storage and native file format encoders.

As an example we will be implementing an S3 loader, hence we will additionally require the `stream-loader-s3` dependency. Any loader is an ordinary JVM application that simply runs an instance of a `StreamLoader` (or multiple instances in separate threads if higher throughput is needed):

```scala
def main(args: Array[String]): Unit = {
  val loader = new StreamLoader(source, sink)
  sys.addShutdownHook { loader.stop() }
  loader.start()
}
```

All loaders require a `source` and a `sink`, where the source is always Kafka:

```scala
val source = KafkaSource
  .builder()
  .consumerProperties(consumerProps)
  .pollTimeout(Duration.ofSeconds(1))
  .topics(Seq("topic"))
  .build()
```

and the sink can be arbitrary, in case of S3 we define it as follows:

```scala
val sink = FileSink
  .builder()
  .recordFormatter((r: Record) => Seq(new String(r.consumerRecord.value(), "UTF-8"))) // (1)
  .fileBuilderFactory(new CsvFileBuilderFactory[String](Compression.ZSTD))            // (2)
  .fileCommitStrategy(FileCommitStrategy.ReachedAnyOf(recordsWritten = 1000))         // (3)
  .fileStorage(
    S3FileStorage          // (4)
      .builder()
      .s3Client(s3Client)  // (5)
      .bucket("test")      // (6)
      .filePathFormatter(
        new TimePartitioningFilePathFormatter(   // (7)
          Some("'dt='yyyyMMdd"),                 // (8)
          Some("zst")                            // (9)
        ))
      .build()
  )
  .build()
```

Reading this from top to bottom we see that this loader will interpret the bytes of incoming Kafka messages as UTF-8 strings _(1)_, construct Zstd compressed CSV files containing these strings _(2)_ and will close and commit them once a 1000 records is written _(3)_. The files will get stored to S3 _(4)_ using the specified `s3Client` instance _(5)_ to the `test` bucket _(6)_. The stored files will be time partitioned _(7)_ based on the watermark of the Kafka message timestamps, the partition prefix will look like `dt=20200313/` _(8)_ and the files will have a `zst` extension _(9)_, the names being random UUIDs.

Every part of this sink definition can be customized, e.g. your Kafka messages might contain JSON which you might want to encode to Avro and write to parquet files. You can decide to use a different storage and switch to e.g. HDFS and so on.

For further complete examples you can explore the [loaders](../../tree/master/stream-loader-tests/src/main/scala/com/adform/streamloader/loaders) used for integration testing, a reference loader for every supported storage is available. For more details about implementing custom loaders refer to the [API reference](https://adform.github.io/stream-loader/com/adform/streamloader/index.html).

## Delivery Guarantees

Message processing semantics of any system reading Kafka depends on the way the consumer handles offsets. For applications storing messages to another system, i.e. stream loaders, the question boils down to whether offsets are stored before, after or atomically together with data, which determines whether the loader is at-most, at-least or exactly once.

All the currently bundled storage implementations strive for exactly-once semantics by loading data and offsets together in a single atomic transaction. There are various strategies for achieving this, the most obvious solution being storing the offsets in the data itself, e.g. as extra columns in a database table or in the names of files being transfered to a distributed file system. This is the approach taken by the Vertica and ClickHouse storages.

An alternative approach is to store data in a [two-phase commit](https://en.wikipedia.org/wiki/Two-phase_commit_protocol), which is done in the HDFS and S3 storage implementations. The _prepare_ step consists of staging a file (uploading to a temporary location in HDFS or starting a multi-part upload in S3) and committing new offsets with the staging information to the Kafka offset commit [metadata](https://kafka.apache.org/24/javadoc/org/apache/kafka/clients/consumer/OffsetAndMetadata.html) while retaining the original offsets. The _commit_ step stores the staged file (moves it to the final destination in HDFS or completes the multi-part upload in S3) and commits the new offsets to Kafka, at the same time clearing the staging metadata. In this approach the transaction would be rolled back if the loader crashes before completing the staging phase, or replayed if it crashed after staging.

One possible caveat is the scenario when data committing queues up, in that case the loader can suspend Kafka consumption and eventually this could lead to a rebalance event. The sink will try to cancel all pending commits and clear the queue, but if the in-progress commit does not react to the cancellation and silently succeeds in the background it would result in data duplication. Setting the `max.poll.interval.ms` Kafka consumer property to be much larger than the storage timeout helps alleviate the problem, but in theory it can still happen.

## Performance

The stream loader library provides basically no overhead compared to a stand-alone Kafka consumer, it actually *is* just a Kafka consumer that provides tools and components to sink the stream to storage, hence the the overall performance depends on the configuration of the consumer (number of threads, buffer sizes, poll duration, etc.) and the implementation of the sinking part.

For file based sinks performance can be roughly broken down to:

* Kafka consumer record decoding and processing, which is completely user defined,
* Record encoding and writing to files, which can be provided by stream-loader components,
* Committing files to storage, which is largely storage dependent.

Stream loader comes bundled with builders for common file formats and macro based encoder derivations that implement case class serialization to CSV, [parquet](https://parquet.apache.org/), [native](https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/AdministratorsGuide/BinaryFilesAppendix/CreatingNativeBinaryFormatFiles.htm) Vertica and [row binary](https://clickhouse.tech/docs/en/interfaces/formats/#rowbinary) ClickHouse file formats. These auto derived encoders provide performance equivalent to hand-written code, as they generate straightforward JVM code with little to none object allocation.

## Comparison to Alternatives

There are other options for loading data from Kafka out in the wild, e.g.

- **[Kafka Connect](https://docs.confluent.io/current/connect/index.html)**: conceptually the stream loader library is an alternative to the Sink part of the Connect framework. The advantage of Kafka Connect is the enormous ecosystem around it with tons of available sinks to chose from, some of which also provide exactly-once semantics. The main disadvantage (for some, at least) is the need to maintain a cluster of workers. Deployment is also problematic and requires an ad-hoc flow. For a more detailed discussion see [KIP-304](https://cwiki.apache.org/confluence/display/KAFKA/KIP-304%3A+Connect+runtime+mode+improvements+for+container+platforms).
  In contrast stream loader is a library meaning that you can use existing orchestrators such as Kubernetes to deploy and run your loaders.
- **Built-in database Kafka ingesters**: some databases such as [Vertica](https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/KafkaIntegrationGuide/VerticaAndApacheKafka.htm) and [ClickHouse](https://clickhouse.tech/docs/en/operations/table_engines/kafka/) support direct data ingestion from Kafka. The main disadvantage with this approach is the coupling of data processing to the database which can have a performance impact. These mechanisms usually also provide limited options for parsing and transforming the data that is being loaded. On the other hand, using a built-in ingest mechanism can be very easy to configure and deploy for simple data flows.

In summary, stream loader aims to be a correct, flexible, highly customizable and efficient solution for loading data from Kafka, provided as a set of components for users to assemble and run themselves. Some of these design choices can be considered either advantages or disadvantages, depending on the use case.

## License

Stream Loader is licensed under the [Mozilla Public License Version 2.0](https://www.mozilla.org/en-US/MPL/2.0/).
