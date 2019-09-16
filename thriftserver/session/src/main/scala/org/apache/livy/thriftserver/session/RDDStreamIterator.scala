package org.apache.livy.thriftserver.session

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

class RDDStreamIterator[T: ClassTag] {
  private var rdd: RDD[T] = _
  private var batchSize: Int = _
  private var sc: SparkContext = _
  private var curPartitionIndex: Int = _
  private var maxPartitionIndex: Int = _
  private var curRowIndex: Int = _
  private var partitionSizeList: Array[Int] = _

  private var iter: Iterator[T] = _

  def this(rdd: RDD[T], batchSize: Int, sc: SparkContext) = {
    this()
    this.rdd = rdd
    this.batchSize = batchSize
    this.sc = sc
    this.curPartitionIndex = 0
    this.maxPartitionIndex = rdd.partitions.length - 1
    this.curRowIndex = 0
    this.partitionSizeList = rdd.mapPartitions(iter => Iterator(iter.size), true).collect()
    this.iter = collectPartitionByBatch(rdd, curRowIndex, batchSize, curPartitionIndex)
  }

  def collectPartitionByBatch[T:ClassTag](rdd: RDD[T], curRowIndex: Int, batchSize: Int, curPartitionIndex: Int): Iterator[T] = {
    sc.runJob(rdd, (iter: Iterator[T]) => iter.slice(curRowIndex, curRowIndex + batchSize).toArray, Seq(curPartitionIndex)).head.iterator
  }

  def hasNext: Boolean = {
    if(iter.hasNext) {
      return true
    }

    if(curPartitionIndex > maxPartitionIndex) {
      return false
    }

    iter = collectPartitionByBatch(rdd, curRowIndex, batchSize, curPartitionIndex)
    if(curRowIndex + batchSize >= partitionSizeList(curPartitionIndex)) {
      curPartitionIndex = curPartitionIndex + 1
      curRowIndex = 0
    } else {
      curRowIndex = curRowIndex + batchSize
    }

    iter.hasNext
  }

  def next: T = {
    iter.next()
  }
}