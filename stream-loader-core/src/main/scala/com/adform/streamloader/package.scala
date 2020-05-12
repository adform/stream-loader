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
  * be no way to provide any delivery guarantees). A large class of sinks are file based, i.e. they write records to files
  * using some $FileBuilderFactory and eventually store them to some underlying storage system implemented as a $FileStorage,
  * such as a database or a distributed file system like HDFS.
  *
  * A sketch of the class hierarchy illustrating the main classes and interfaces can be seen below.
  *
  * <br />
  * <object type="image/svg+xml" data="../../../diagrams/core_class_hierarchy.svg" style="display: block; margin: auto"></object>
  * <br />
  *
  * The $LocalFileStorage class is shown here as a reference implementation and probably should not be used in production, for more realistic storage implementations
  * see the [[clickhouse]], [[hadoop]], [[s3]] and [[vertica]] packages. They also contain more file builder implementations than just the $CsvFileBuilderFactory
  * included in the core library.
  *
  * @define FileStorage [[com.adform.streamloader.file.storage.FileStorage FileStorage]]
  * @define FileBuilderFactory [[com.adform.streamloader.file.FileBuilderFactory FileBuilderFactory]]
  * @define LocalFileStorage [[com.adform.streamloader.file.storage.LocalFileStorage LocalFileStorage]]
  * @define CsvFileBuilderFactory [[com.adform.streamloader.encoding.csv.CsvFileBuilderFactory CsvFileBuilderFactory]]
  */
package object streamloader
