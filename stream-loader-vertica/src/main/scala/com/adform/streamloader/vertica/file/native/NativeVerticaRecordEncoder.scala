/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.vertica.file.native

import java.time.{LocalDate, LocalDateTime}
import java.util.UUID

import com.adform.streamloader.sink.encoding.macros.DataTypeEncodingAnnotation.{DecimalEncoding, FixedLength, MaxLength}
import com.adform.streamloader.model.Timestamp
import com.adform.streamloader.sink.encoding.macros.{DataTypeEncodingAnnotation, RecordFieldExtractor}

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/**
  * An encoder for encoding records to rows in the native Vertica file format.
  * Implementations need to define the encoding by composing calls to a provided primitive type writer,
  * static column sizes and nullability of each field have to be defined separately.
  *
  * @tparam R Type of records being encoded.
  */
trait NativeVerticaRecordEncoder[R] {

  /**
    * Gets an array of static data type sizes for each column.
    * Variable length columns have static sizes of -1.
    */
  def staticColumnSizes: Array[Int]

  /**
    * Invokes the provided `setter` method for each column that is null in a given record,
    * provides the ordinal position of the column as an argument to the setter.
    */
  def setNullBits(record: R, setter: Int => Unit): Unit

  /**
    * Writes a given record using the provided primitive type writer.
    */
  def write(record: R, primitiveTypeWriter: NativeVerticaPrimitiveTypeWriter): Unit
}

object NativeVerticaRecordEncoder {

  private val MAX_COLUMN_BYTES = 65000
  private val MAX_COLUMN_LENGTH = MaxLength(MAX_COLUMN_BYTES, truncate = true)

  /**
    * A macro derivation of the encoder for arbitrary case classes.
    */
  implicit def apply[R <: Product]: NativeVerticaRecordEncoder[R] = macro applyImpl[R]

  def applyImpl[R: c.WeakTypeTag](c: blackbox.Context): c.Expr[NativeVerticaRecordEncoder[R]] = {
    import c.universe._

    val recordType = weakTypeOf[R]

    case class Column(isNull: c.Tree, staticSize: c.Tree, writeExpr: c.Tree)

    def columnExpressions(getter: MethodSymbol, typeAnnotations: Seq[DataTypeEncodingAnnotation]): Column = {
      def primitiveColumnExpressions(tpe: c.Type): Column =
        tpe match {
          case t if t =:= c.weakTypeOf[Boolean] => Column(q"false", q"1", q"pw.writeByte(if (r) 1 else 0)")
          case t if t =:= c.weakTypeOf[Byte] => Column(q"false", q"1", q"pw.writeByte(r)")
          case t if t =:= c.weakTypeOf[Short] => Column(q"false", q"2", q"pw.writeInt16(r)")
          case t if t =:= c.weakTypeOf[Int] => Column(q"false", q"4", q"pw.writeInt32(r)")
          case t if t =:= c.weakTypeOf[Long] => Column(q"false", q"8", q"pw.writeInt64(r)")
          case t if t =:= c.weakTypeOf[Float] => Column(q"false", q"8", q"pw.writeFloat64(r)")
          case t if t =:= c.weakTypeOf[Double] => Column(q"false", q"8", q"pw.writeFloat64(r)")
          case t if t =:= c.weakTypeOf[Char] => Column(q"false", q"1", q"pw.writeByte(r)")
          case t if t =:= c.weakTypeOf[Timestamp] => Column(q"false", q"8", q"pw.writeTimestamp(r)")
          case t if t =:= c.weakTypeOf[LocalDateTime] => Column(q"false", q"8", q"pw.writeTimestamp(r)")
          case t if t =:= c.weakTypeOf[LocalDate] => Column(q"false", q"8", q"pw.writeDate(r)")
          case t if t =:= c.weakTypeOf[UUID] => Column(q"false", q"16", q"pw.writeUUID(r)")

          case t if t =:= c.weakTypeOf[String] =>
            val fl = typeAnnotations.collectFirst { case f: FixedLength => f }
            if (fl.isDefined) {
              if (fl.get.length > MAX_COLUMN_BYTES)
                c.abort(c.enclosingPosition, s"String length can not exceed $MAX_COLUMN_BYTES")
              Column(q"false", q"${fl.get.length}", q"pw.writeFixedString(r, ${fl.get.length}, ${fl.get.truncate})")
            } else {
              val ml = typeAnnotations.collectFirst { case m: MaxLength => m }.getOrElse(MAX_COLUMN_LENGTH)
              if (ml.length > MAX_COLUMN_BYTES)
                c.abort(c.enclosingPosition, s"String length can not exceed $MAX_COLUMN_BYTES")
              Column(q"false", q"-1", q"pw.writeVarString(r, ${ml.length}, ${ml.truncate})")
            }

          case t if t =:= c.weakTypeOf[Array[Byte]] =>
            val fl = typeAnnotations.collectFirst { case f: FixedLength => f }
            if (fl.isDefined) {
              if (fl.get.length > MAX_COLUMN_BYTES)
                c.abort(c.enclosingPosition, s"Byte array length can not exceed $MAX_COLUMN_BYTES")
              Column(
                q"false",
                q"${fl.get.length}",
                q"pw.writeFixedByteArray(r, ${fl.get.length}, ${fl.get.truncate}, 0)"
              )
            } else {
              val ml = typeAnnotations.collectFirst { case m: MaxLength => m }.getOrElse(MAX_COLUMN_LENGTH)
              if (ml.length > MAX_COLUMN_BYTES)
                c.abort(c.enclosingPosition, s"Byte array length can not exceed $MAX_COLUMN_BYTES")
              Column(q"false", q"-1", q"pw.writeVarByteArray(r, ${ml.length}, ${ml.truncate})")
            }

          case t if t =:= c.weakTypeOf[BigDecimal] =>
            val enc = typeAnnotations.collectFirst { case e @ DecimalEncoding(_, _) => e }
            if (enc.isEmpty) c.abort(c.enclosingPosition, "Decimal columns must have the DecimalEncoding annotation")
            val size = math.ceil((((enc.get.precision / 19) + 1) * 8).toDouble).toInt
            Column(q"false", q"$size", q"pw.writeDecimal(r, ${enc.get.precision}, ${enc.get.scale})")

          case t =>
            val nte =
              q"implicitly[_root_.com.adform.streamloader.vertica.file.native.NativeVerticaTypeEncoder[${t.finalResultType}]]"
            Column(q"false", q"$nte.staticSize", q"$nte.write(r, pw)")
        }

      getter.returnType match {
        case t if t.typeConstructor =:= c.weakTypeOf[Option[_]].typeConstructor =>
          val primitive = primitiveColumnExpressions(t.typeArgs.head)
          Column(
            q"record.$getter.isEmpty",
            primitive.staticSize,
            q"if (record.$getter.isDefined) { val r = record.$getter.get; ${primitive.writeExpr} }"
          )
        case t =>
          val primitive = primitiveColumnExpressions(t)
          Column(
            primitive.isNull,
            primitive.staticSize,
            q"{ val r = record.$getter; ${primitive.writeExpr} }"
          )
      }
    }

    val fields = RecordFieldExtractor.fromCaseClass[R](c)
    val columns = fields.map(f => columnExpressions(f.getter, f.typeAnnotations))

    val encoderType = weakTypeOf[NativeVerticaRecordEncoder[R]]
    val ptwType = weakTypeOf[NativeVerticaPrimitiveTypeWriter]

    c.Expr[NativeVerticaRecordEncoder[R]](q"""
       new $encoderType {
         override def staticColumnSizes: _root_.scala.Array[Int] = _root_.scala.Array(..${columns.map(_.staticSize)})
         override def setNullBits(record: $recordType, setter: Int => Unit): Unit = {
           ..${columns.zipWithIndex.map { case (col, idx) => q"if (${col.isNull}) setter($idx)" }}
         }
         override def write(record: $recordType, pw: $ptwType): Unit = { ..${columns.map(_.writeExpr)} }
       }
    """)
  }
}
