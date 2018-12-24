package spark

import kafka.producer.Partitioner

class CustomPartitioner extends Partitioner {
  override def partition(key: Any, numPartitions: Int): Int = key.hashCode() % numPartitions
}
