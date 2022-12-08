/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.encoding.macros

import scala.annotation.StaticAnnotation
import scala.reflect.macros.blackbox

/**
  * A data type annotation that can influence the way their values are encoded,
  * used by annotating case class member types, e.g.
  *
  * {{{
  *   case class ExampleRow(
  *     name: String @MaxLength(100),
  *     countryCode: Option[String] @FixedLength(2, truncate = false),
  *     salary: BigDecimal @DecimalEncoding(18, 10)
  *   )
  * }}}
  *
  * It is up to specific encoders to actually take these annotations into account and
  * alter encoding based on their presence.
  */
sealed trait DataTypeEncodingAnnotation extends StaticAnnotation

object DataTypeEncodingAnnotation {
  private val TRUNCATE_BY_DEFAULT = true

  /**
    * Precision and scale specification for [[scala.math.BigDecimal]] values, `DECIMAL(p, s)` in most databases.
    */
  case class DecimalEncoding(precision: Int, scale: Int) extends DataTypeEncodingAnnotation

  /**
    * Maximum length specification for strings and byte arrays, `VARCHAR(m)` and `VARBINARY(m)` in most databases.
    */
  case class MaxLength(length: Int, truncate: Boolean) extends DataTypeEncodingAnnotation {
    def this(length: Int) =
      this(
        length,
        TRUNCATE_BY_DEFAULT
      ) // default parameter values don't play well with macros, so define an extra constructor
  }

  /**
    * Fixed length specification for strings and byte arrays, `CHAR(m)` and `BINARY(m)` in most databases.
    */
  case class FixedLength(length: Int, truncate: Boolean) extends DataTypeEncodingAnnotation {
    def this(length: Int) =
      this(
        length,
        TRUNCATE_BY_DEFAULT
      ) // default parameter values don't play well with macros, so define an extra constructor
  }

  /**
    * Reconstructs a [[DataTypeEncodingAnnotation]] instance from a macro Annotation tree.
    */
  def fromTree(c: blackbox.Context)(macroAnnotation: c.universe.Annotation): DataTypeEncodingAnnotation = {
    import c.universe._

    def extractLengthSpecs(constructorParams: List[Tree]): (Int, Boolean) = {
      val (l, t) = constructorParams match {
        case List(Literal(Constant(size))) => (size.asInstanceOf[Int], TRUNCATE_BY_DEFAULT)
        case List(Literal(Constant(size)), Literal(Constant(t))) => (size.asInstanceOf[Int], t.asInstanceOf[Boolean])
        case _ =>
          c.abort(
            c.enclosingPosition,
            s"Length type annotations must be used with a constant size integer and an optional truncation boolean"
          )
      }
      if (l < 1) c.abort(c.enclosingPosition, s"Column length must be greater or equal to 1")
      (l, t)
    }

    macroAnnotation match {
      case a if a.tree.tpe =:= c.weakTypeOf[DecimalEncoding] =>
        val (p, s) = a.tree.children.tail match {
          case List(Literal(Constant(precision)), Literal(Constant(scale))) =>
            (precision.asInstanceOf[Int], scale.asInstanceOf[Int])
          case _ => c.abort(c.enclosingPosition, s"Precision and scale must be literal constant integers")
        }
        if (p < 1 || p > 1024) c.abort(c.enclosingPosition, s"Decimal precision must be between 1 and 1024")
        if (s > p) c.abort(c.enclosingPosition, s"Decimal scale must be less than precision")
        DecimalEncoding(p, s)
      case a if a.tree.tpe =:= c.weakTypeOf[MaxLength] =>
        val (length, truncate) = extractLengthSpecs(a.tree.children.tail)
        MaxLength(length, truncate)
      case a if a.tree.tpe =:= c.weakTypeOf[FixedLength] =>
        val (length, truncate) = extractLengthSpecs(a.tree.children.tail)
        FixedLength(length, truncate)
    }
  }
}
