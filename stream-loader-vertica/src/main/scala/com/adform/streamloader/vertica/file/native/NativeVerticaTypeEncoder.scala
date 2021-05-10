/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.vertica.file.native

/**
  * Encoder of arbitrary data types to the native Vertica file format.
  * The macro derived case class [[NativeVerticaRecordEncoder]] looks up type encoders implicitly for unknown types,
  * this way arbitrary data types can be encoded.
  *
  * @tparam T Type being encoded.
  */
trait NativeVerticaTypeEncoder[-T] {

  /**
    * Gets the size of the data type in bytes.
    */
  def staticSize: Int

  /**
    * Invokes calls to the provided primitive type writer needed to encode the given data type value.
    */
  def write(t: T, primitiveTypeWriter: NativeVerticaPrimitiveTypeWriter): Unit
}
