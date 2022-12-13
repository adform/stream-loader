/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.util

import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet

import java.util

/**
  *   Not thread safe
  *
  *   FIFO Hash set storing keys, removes last element while adding new element.
  *   Adds key to beginning, if key already exists it will be moved to beginning of the set
  */
class FifoHashSet[A] private (maxEntries: Int, set: ObjectLinkedOpenHashSet[A]) extends util.AbstractSet[A] {

  override def add(e: A): Boolean = {
    if (size() >= maxEntries) {
      set.removeLast()
    }
    set.addAndMoveToFirst(e)
  }

  override def contains(o: Any): Boolean = set.contains(o)

  override def clear(): Unit = set.clear()

  override def size(): Int = set.size()

  override def iterator(): util.Iterator[A] = set.iterator()
}

object FifoHashSet {

  def apply[A](maxEntries: Int): FifoHashSet[A] = {
    val set = new ObjectLinkedOpenHashSet[A](maxEntries)
    new FifoHashSet(maxEntries, set)
  }
}
