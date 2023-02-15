package akcl

import org.apache.kafka.clients.consumer.{Consumer, ConsumerRebalanceListener, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import java.time.Duration
import java.util
import java.util.Properties
import java.util.regex.Pattern
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class UpsertKafkaClient[K, V](properties: Properties) extends KafkaConsumer[K, V](properties) {

  assert(
    properties.get("enable.auto.commit") == false,
    "UpsertKafkaClient should not commit automatically"
  )

  var notPollYet                                            = true
  var rebalanced                                            = false
  var currentOffsets: mutable.Map[TopicPartition, Long]     = mutable.Map.empty
  var expectedEndOffsets: mutable.Map[TopicPartition, Long] = mutable.Map.empty
  var snapshotMap: mutable.Map[K, ConsumerRecord[K, V]]     = mutable.Map.empty

  override def subscribe(topics: util.Collection[String], listener: ConsumerRebalanceListener): Unit = {
    val rebalanceTracker = new ConsumerRebalanceListener {
      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = rebalanced = true

      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = rebalanced = true
    }
    super.subscribe(topics, new CompositeRebalanceListener(rebalanceTracker, listener))
  }

  override def subscribe(
      pattern: Pattern,
      listener: ConsumerRebalanceListener
  ): Unit = {
    val rebalanceTracker = new ConsumerRebalanceListener {
      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = rebalanced = true

      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = rebalanced = true
    }

    super.subscribe(
      pattern,
      new CompositeRebalanceListener(rebalanceTracker, listener)
    )
  }

  /** If polling 1st time:
    *
    *   1. store starting offset
    *   1. store current end off set
    *   1. if consumed record not >= end offset
    *      1. poll
    *      1. check if rebalance happens, if so
    *         a. unsubscribe
    *         a. resubscribe
    *         a. go back to starting point
    *      1. compact polled records, keep in mem
    *   1. Once consumed to end offset stored previously, convert in mem records into ConsumerRecords and return
    *
    * This method can take a long time!!
    */
  override def poll(timeout: Duration): ConsumerRecords[K, V] = {
    if (notPollYet) {
      val assignedTopicPartitions = assignment()
      val startingOffsets         = committed(assignedTopicPartitions).asScala

      expectedEndOffsets = endOffsets(assignedTopicPartitions).asScala
        .map { case (k, v) => k -> v.longValue() }

      var records: ConsumerRecords[K, V] = null

      do {
        if (rebalanced) {
          clearInternalState()
          startingOffsets.foreach { case (tp, offset) => seek(tp, offset) }
        }
        records = super.poll(Duration.ofMillis(500))
        compactAndTrackOffsets(records)
      } while (!records.isEmpty && reachEndOffset(
        expectedEndOffsets,
        currentOffsets
      ))
      recordsFromCompacted()
    } else {
      super.poll(timeout)
    }
  }

  private def recordsFromCompacted() = {
    val recordsJavaMap = new util.HashMap[TopicPartition, util.List[ConsumerRecord[K, V]]]()
    snapshotMap.foreach { case (key, record) =>
      val tp       = new TopicPartition(record.topic(), record.partition())
      val partList = recordsJavaMap.get(tp)
      if (partList == null) {
        val newList = new util.ArrayList[ConsumerRecord[K, V]]()
        newList.add(record)
        recordsJavaMap.put(tp, newList)
      } else {
        partList.add(record)
      }
    }
    new ConsumerRecords[K, V](recordsJavaMap)
  }

  private def clearInternalState() = {
    notPollYet = true
    rebalanced = false
    currentOffsets = mutable.Map.empty
    snapshotMap = mutable.Map.empty

  }

  private def compactAndTrackOffsets(records: ConsumerRecords[K, V]) = {
    records.forEach { rec =>
      snapshotMap.update(rec.key(), rec)
      currentOffsets.update(
        new TopicPartition(rec.topic(), rec.partition()),
        rec.offset()
      )
    }
  }

  private def reachEndOffset(
      targetOffsets: mutable.Map[TopicPartition, Long],
      currentOffsets: mutable.Map[TopicPartition, Long]
  ): Boolean = {
    targetOffsets.forall { case (partition, targetOffset) =>
      currentOffsets.get(partition) match {
        case Some(currentOffset) => currentOffset > targetOffset
        case None                => false
      }
    }
  }
}
