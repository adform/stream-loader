/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform

/**
  * The entry point of the stream loader library is the [[StreamLoader]] class, which requires a [[KafkaSource]] and a [[Sink]]. Once started
  * it will subscribe to the provided topics and will start polling and sinking records.
  * The sink has to be able to persist records and to look up committed offsets (technically this is optional, but without it there would
  * be no way to provide any delivery guarantees). A large class of sinks are batch based, implemented as $RecordBatchingSink.
  * This sink accumulate batches of records using some $RecordBatcher and once ready, stores them to some underlying $RecordBatchStorage.
  * A common type of batch is file based, i.e. a batcher might write records to a temporary file and once the file is full
  * the sink commits the file to some underlying storage, such as a database or a distributed file system like HDFS.
  *
  * A sketch of the class hierarchy illustrating the main classes and interfaces can be seen below.
  *
  * <br />
  * <object type="image/svg+xml" data="../../../diagrams/core_class_hierarchy.svg" style="display: block; margin: auto"></object>
  * <br />
  *
  * For concrete storage implementations see the [[clickhouse]], [[hadoop]], [[s3]] and [[vertica]] packages.
  * They also contain more file builder implementations than just the $CsvFileBuilder included in the core library.
  *
  * @define RecordBatchingSink [[com.adform.streamloader.batch.RecordBatchingSink RecordBatchingSink]]
  * @define RecordBatcher [[com.adform.streamloader.batch.RecordBatcher RecordBatcher]]
  * @define RecordBatchStorage [[com.adform.streamloader.batch.storage.RecordBatchStorage RecordBatchStorage]]
  * @define CsvFileBuilder [[com.adform.streamloader.encoding.csv.CsvFileBuilder CsvFileBuilder]]
  */
package object streamloader
