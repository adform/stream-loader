/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.encoding.csv

import com.adform.streamloader.sink.encoding.macros.RecordFieldExtractor

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/**
  * An encoder of some type `R` to lines in a CSV file.
  *
  * @tparam R Type of records to encode.
  */
trait CsvRecordEncoder[R] {

  /**
    * List of record column names, used to write the header, if needed.
    */
  val columnNames: Array[String]

  /**
    * Encodes a given record to an array of strings.
    */
  def encode(record: R): Array[Option[String]]

  /**
    * Encodes a record to an array of strings.
    * Throws if the encoding returns more values that there are column names.
    */
  def safeEncode(record: R): Array[Option[String]] = {
    val result = encode(record)
    result ensuring (result.length == columnNames.length)
  }
}

object CsvRecordEncoder {

  /**
    * An encoder for strings that simply outputs them as a single column named "value".
    */
  implicit val stringEncoder: CsvRecordEncoder[String] = new CsvRecordEncoder[String] {
    override val columnNames: Array[String] = Array("value")
    override def encode(record: String): Array[Option[String]] = Array(Option(record))
  }

  /**
    * A macro derivation of an encoder for arbitrary case classes.
    * Each case class field is output as a column, names correspond to case class field names.
    * If the field data type conversion to a string is not defined, a [[CsvTypeEncoder]] for the type
    * is searched for implicitly.
    */
  implicit def productEncoder[R <: Product]: CsvRecordEncoder[R] = macro productEncoderImpl[R]

  def productEncoderImpl[R: c.WeakTypeTag](c: blackbox.Context): c.Expr[CsvRecordEncoder[R]] = {
    import c.universe._

    val recordType = weakTypeOf[R]
    val encoderType = weakTypeOf[CsvRecordEncoder[R]]

    def columnSelector(getter: MethodSymbol): c.Tree = {
      def primitiveColumnSelector(tpe: c.Type): c.Tree = tpe match {
        case t if t =:= c.weakTypeOf[Byte] => q"_.toString"
        case t if t =:= c.weakTypeOf[Short] => q"_.toString"
        case t if t =:= c.weakTypeOf[Int] => q"_.toString"
        case t if t =:= c.weakTypeOf[Long] => q"_.toString"
        case t if t =:= c.weakTypeOf[Char] => q"_.toString"
        case t if t =:= c.weakTypeOf[String] => q"(x: String) => x"
        case t =>
          val te = q"implicitly[_root_.com.adform.streamloader.sink.encoding.csv.CsvTypeEncoder[${t.finalResultType}]]"
          q"(x: ${t.finalResultType}) => $te.encode(x)"
      }

      getter.returnType match {
        case t if t.typeConstructor =:= c.weakTypeOf[Option[_]].typeConstructor =>
          q"record.$getter.map(${primitiveColumnSelector(t.typeArgs.head)})"
        case t =>
          q"_root_.scala.Option(record.$getter).map(${primitiveColumnSelector(t)})"
      }
    }

    val fields = RecordFieldExtractor.fromCaseClass[R](c)
    val columns = fields.map(f => (f.getter.name.toString, columnSelector(f.getter)))

    c.Expr[CsvRecordEncoder[R]](q"""new $encoderType {
        override val columnNames: _root_.scala.Array[String] = _root_.scala.Array(..${columns.map(_._1)})
        override def encode(record: $recordType): _root_.scala.Array[_root_.scala.Option[String]] =
          _root_.scala.Array(..${columns.map(_._2)})
      }
    """)
  }
}
