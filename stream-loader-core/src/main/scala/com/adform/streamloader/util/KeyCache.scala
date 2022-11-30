package com.adform.streamloader.util

class KeyCache[A] private (cacheSize: Int) {

  var cacheForPartition: Map[Int, FifoHashSet[A]] = _

  def initCache(partitions: Set[Int]): Unit = {
    cacheForPartition = partitions.map(p => (p, FifoHashSet[A](cacheSize))).toMap
  }

  def clearCache(): Unit = cacheForPartition.values.foreach(_.clear())

  def add(partition: Int, key: A): Unit = cacheForPartition(partition).add(key)

  def contains(partition: Int, key: A): Boolean = cacheForPartition(partition).contains(key)

  def partitionReady(partition: Int): Boolean = cacheSize == cacheForPartition(partition).size()

}

object KeyCache {
  def apply[A](cacheSize: Int): KeyCache[A] = new KeyCache[A](cacheSize)
}
