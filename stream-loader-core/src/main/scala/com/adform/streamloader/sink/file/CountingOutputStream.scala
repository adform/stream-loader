/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.file

import java.io.OutputStream

/**
  * An `OutputStream` wrapper that also counts the number of bytes written.
  */
class CountingOutputStream(os: OutputStream) extends OutputStream {
  var size = 0L

  override def write(i: Int): Unit = {
    size += 1
    os.write(i)
  }

  override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
    size += len
    os.write(bytes, off, len)
  }

  override def write(bytes: Array[Byte]): Unit = {
    size += bytes.length
    os.write(bytes)
  }

  override def close(): Unit = {
    os.close()
  }

  override def flush(): Unit = {
    os.flush()
  }
}
