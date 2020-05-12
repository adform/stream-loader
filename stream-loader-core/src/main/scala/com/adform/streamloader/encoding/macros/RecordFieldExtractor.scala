/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.encoding.macros

import scala.reflect.macros.blackbox

object RecordFieldExtractor {

  /**
    * A record field represented as a getter method and it's encoding annotations.
    */
  case class RecordField[M](getter: M, typeAnnotations: Seq[DataTypeEncodingAnnotation])

  /**
    * Extracts record fields (getters and their encoding annotations) from a case class.
    */
  def fromCaseClass[R: c.WeakTypeTag](c: blackbox.Context): Seq[RecordField[c.universe.MethodSymbol]] = {
    import c.universe._

    val recordType = weakTypeOf[R]

    // Find all getters in the class that match constructor parameters by name, these are the case class getters

    val constructor = recordType.members.find(m => m.isConstructor && m.name.toString == "<init>").map(_.asMethod)
    val ctorParams = constructor match {
      case Some(ctor) => ctor.paramLists.head
      case None =>
        c.abort(c.enclosingPosition, s"Could not find a constructor for type $recordType, is it a case class?")
    }
    val getterMembers =
      ctorParams.map(p => recordType.members.find(m => m.isMethod && m.asMethod.isGetter && m.name == p.name))

    val getters =
      if (getterMembers.forall(_.isDefined)) getterMembers.map(_.get.asMethod)
      else {
        c.abort(
          c.enclosingPosition,
          s"Not all constructor arguments have corresponding fields, are you sure that $recordType is a case class?"
        )
      }

    // Find all type annotations defined on case class members
    val fieldTypeAnnotations = recordType.decls
      .filter(m => !m.isMethod)
      .map(m =>
        (m.name.toString.trim, m.typeSignature.resultType match {
          case t: AnnotatedType => t.annotations.map(a => DataTypeEncodingAnnotation.fromTree(c)(a))
          case _ => Seq.empty
        }))
      .toMap

    getters.map(g => RecordField(g, fieldTypeAnnotations(g.name.toString)))
  }
}
