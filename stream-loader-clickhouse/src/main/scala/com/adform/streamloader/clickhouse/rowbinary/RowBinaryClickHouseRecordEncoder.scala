/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.clickhouse.rowbinary

import java.time.{LocalDate, LocalDateTime}
import java.util.UUID

import com.adform.streamloader.sink.encoding.macros.DataTypeEncodingAnnotation.{DecimalEncoding, FixedLength, MaxLength}
import com.adform.streamloader.model.Timestamp
import com.adform.streamloader.sink.encoding.macros.{DataTypeEncodingAnnotation, RecordFieldExtractor}

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/**
  * An encoder for the RowBinary ClickHouse file format.
  * Implementations need to define the encoding by composing calls to a provided primitive type writer.
  *
  * @tparam R Type of records being encoded.
  */
trait RowBinaryClickHouseRecordEncoder[R] {
  def write(record: R, pw: RowBinaryClickHousePrimitiveTypeWriter): Unit
}

object RowBinaryClickHouseRecordEncoder {

  /**
    * A macro derivation of the encoder for arbitrary case classes.
    */
  implicit def apply[R <: Product]: RowBinaryClickHouseRecordEncoder[R] = macro applyImpl[R]

  def applyImpl[R: c.WeakTypeTag](c: blackbox.Context): c.Expr[RowBinaryClickHouseRecordEncoder[R]] = {
    import c.universe._

    val recordType = weakTypeOf[R]

    def isComposite(tpe: c.Type): Boolean =
      tpe.typeConstructor =:= c.weakTypeOf[Option[_]].typeConstructor ||
        tpe.typeConstructor =:= c.weakTypeOf[Array[_]].typeConstructor

    def columnExpressions(getter: MethodSymbol, typeAnnotations: Seq[DataTypeEncodingAnnotation]): c.Tree = {
      def primitiveColumnExpressions(tpe: c.Type): c.Tree = {
        tpe match {
          case t if t =:= c.weakTypeOf[Boolean] => q"pw.writeByte(if (r) 1 else 0)"
          case t if t =:= c.weakTypeOf[Byte] => q"pw.writeByte(r)"
          case t if t =:= c.weakTypeOf[Short] => q"pw.writeInt16(r)"
          case t if t =:= c.weakTypeOf[Int] => q"pw.writeInt32(r)"
          case t if t =:= c.weakTypeOf[Long] => q"pw.writeInt64(r)"
          case t if t =:= c.weakTypeOf[Float] => q"pw.writeFloat32(r)"
          case t if t =:= c.weakTypeOf[Double] => q"pw.writeFloat64(r)"
          case t if t =:= c.weakTypeOf[Char] => q"pw.writeByte(r)"
          case t if t =:= c.weakTypeOf[Timestamp] => q"pw.writeDateTime(r)"
          case t if t =:= c.weakTypeOf[LocalDateTime] => q"pw.writeDateTime(r)"
          case t if t =:= c.weakTypeOf[LocalDate] => q"pw.writeDate(r)"
          case t if t =:= c.weakTypeOf[UUID] => q"pw.writeUuid(r)"

          case t if t =:= c.weakTypeOf[String] =>
            val fl = typeAnnotations.collectFirst { case f: FixedLength => f }
            if (fl.isDefined) {
              q"pw.writeFixedString(r, ${fl.get.length}, ${fl.get.truncate})"
            } else {
              val ml = typeAnnotations.collectFirst { case m: MaxLength => m }
              if (ml.isDefined) {
                q"pw.writeString(r, ${ml.get.length}, ${ml.get.truncate})"
              } else {
                q"pw.writeString(r)"
              }
            }

          case t if t =:= c.weakTypeOf[BigDecimal] =>
            val enc = typeAnnotations.collectFirst { case e @ DecimalEncoding(_, _) => e }
            if (enc.isEmpty) c.abort(c.enclosingPosition, "Decimal columns must have the DecimalEncoding annotation")
            q"pw.writeDecimal(r, ${enc.get.precision}, ${enc.get.scale})"

          case t =>
            val nte =
              q"implicitly[_root_.com.adform.streamloader.clickhouse.rowbinary.RowBinaryClickHouseRecordEncoder[${t.finalResultType}]]"
            q"$nte.write(r, pw)"
        }
      }

      getter.returnType match {
        case t if t.typeConstructor =:= c.weakTypeOf[Array[_]].typeConstructor =>
          val argType = t.typeArgs.head
          if (argType.typeConstructor =:= c.weakTypeOf[Byte].typeConstructor) {
            val fl = typeAnnotations.collectFirst { case f: FixedLength => f }
            if (fl.isDefined) {
              q"pw.writeLeb128(${fl.get.length}); pw.writeFixedByteArray(record.$getter, ${fl.get.length}, ${fl.get.truncate}, 0)"
            } else {
              val ml = typeAnnotations.collectFirst { case m: MaxLength => m }
              if (ml.isDefined) {
                q"pw.writeLeb128(Math.min(${ml.get.length}, record.$getter.length)); pw.writeByteArray(record.$getter, ${ml.get.length}, ${ml.get.truncate})"
              } else {
                q"pw.writeLeb128(record.$getter.length); pw.writeByteArray(record.$getter)"
              }
            }
          } else if (argType.typeConstructor =:= c.weakTypeOf[Option[_]].typeConstructor) {
            val nestedType = argType.typeArgs.head
            if (isComposite(nestedType)) c.abort(c.enclosingPosition, "Composite data types cannot be optional.")
            val primitiveExpr = primitiveColumnExpressions(nestedType)
            q"{ pw.writeLeb128(record.$getter.length); record.$getter.foreach { opt => { if (opt.isDefined) { pw.writeByte(0); val r = opt.get; $primitiveExpr } else pw.writeByte(1) } } }"
          } else if (argType.typeConstructor =:= c.weakTypeOf[Array[_]].typeConstructor) {
            val nestedType = argType.typeArgs.head
            if (isComposite(nestedType)) c.abort(c.enclosingPosition, "Nesting more than two levels is not supported.")
            val primitiveExpr = primitiveColumnExpressions(nestedType)
            q"{ pw.writeLeb128(record.$getter.length); record.$getter.foreach { arr => { pw.writeLeb128(arr.length); arr.foreach { r => $primitiveExpr }}}}"
          } else {
            val primitiveExpr = primitiveColumnExpressions(argType)
            q"{ pw.writeLeb128(record.$getter.length); record.$getter.foreach { r => $primitiveExpr } }"
          }

        case t if t.typeConstructor =:= c.weakTypeOf[Option[_]].typeConstructor =>
          if (isComposite(t.typeArgs.head)) c.abort(c.enclosingPosition, "Composite data types cannot be optional.")

          val primitiveExpr = primitiveColumnExpressions(t.typeArgs.head)
          q"if (record.$getter.isDefined) { pw.writeByte(0); val r = record.$getter.get; $primitiveExpr } else pw.writeByte(1)"

        case t =>
          val primitive = primitiveColumnExpressions(t)
          q"{ val r = record.$getter; $primitive }"
      }
    }

    val fields = RecordFieldExtractor.fromCaseClass[R](c)
    val columns = fields.map(f => columnExpressions(f.getter, f.typeAnnotations))

    val encoderType = weakTypeOf[RowBinaryClickHouseRecordEncoder[R]]
    val ptwType = weakTypeOf[RowBinaryClickHousePrimitiveTypeWriter]

    c.Expr[RowBinaryClickHouseRecordEncoder[R]](q"""
       new $encoderType {
         override def write(record: $recordType, pw: $ptwType): Unit = { ..$columns }
       }
    """)
  }
}
