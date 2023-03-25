package akcl

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import java.time.Duration
import java.util
import java.util.Properties
import java.util.regex.Pattern
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class UpsertKafkaConsumer[K, V](properties: Properties) extends KafkaConsumer[K, V](properties) {

  assert(
    properties.get("enable.auto.commit") == "false",
    "UpsertKafkaClient should not commit automatically"
  )

  // transitionary state, use to control behavior
  var needRefreshTargetOffset            = false
  var newPartitions: Set[TopicPartition] = Set()

  // state used to track progress
  var currentOffsets: mutable.Map[TopicPartition, Long]                              = mutable.Map.empty
  var targetOffsets: mutable.Map[TopicPartition, Long]                               = mutable.Map.empty
  var snapshotMap: mutable.Map[TopicPartition, mutable.Map[K, ConsumerRecord[K, V]]] = mutable.Map.empty

  val rebalanceTracker = new ConsumerRebalanceListener {
    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
      needRefreshTargetOffset = true
      partitions.forEach(tp => {
        currentOffsets.remove(tp)
        targetOffsets.remove(tp)
        snapshotMap.remove(tp)
      })
    }

    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
      needRefreshTargetOffset = true
      newPartitions = partitions.asScala.toSet.diff(targetOffsets.keySet)
    }
  }

  override def subscribe(topics: util.Collection[String], listener: ConsumerRebalanceListener): Unit = {
    super.subscribe(topics, new CompositeRebalanceListener(rebalanceTracker, listener))
  }

  override def subscribe(
      pattern: Pattern,
      listener: ConsumerRebalanceListener
  ): Unit = {
    super.subscribe(
      pattern,
      new CompositeRebalanceListener(rebalanceTracker, listener)
    )
  }

  // todo: synchronization??
  override def poll(timeout: Duration): ConsumerRecords[K, V] = {
    do {
      compactAndTrackOffsets(super.poll(timeout))
      if (needRefreshTargetOffset) {
        refetchTargetOffsets()
        needRefreshTargetOffset = false
      }
    } while (!reachEndOffset())

    val output = recordsFromCompacted()
    snapshotMap.clear()
    output
  }

  private def refetchTargetOffsets() = {
    this
      .endOffsets(newPartitions.asJava)
      .forEach((tp, endOffset) => this.targetOffsets.put(tp, endOffset - 1))

    newPartitions = Set.empty
  }

  private def recordsFromCompacted() = {
    val recordsJavaMap = new util.HashMap[TopicPartition, util.List[ConsumerRecord[K, V]]]()
    snapshotMap.foreach { case (tp, kv) =>
      val arrLs = new util.ArrayList[ConsumerRecord[K, V]]()
      kv.foreach { case (k, rec) => arrLs.add(rec) }
      recordsJavaMap.put(tp, arrLs)
    }
    new ConsumerRecords[K, V](recordsJavaMap)
  }

  private def compactAndTrackOffsets(records: ConsumerRecords[K, V]): Unit = {

    for (tp <- records.partitions().asScala) {
      var lastRecOffset = -1L
      for (rec <- records.records(tp).asScala) {
        snapshotMap.updateWith(tp) {
          case Some(value) =>
            value.put(rec.key(), rec)
            Some(value)
          case None => Some(mutable.HashMap(rec.key() -> rec))
        }
        lastRecOffset = rec.offset()
      }
      if (lastRecOffset != -1L) {
        currentOffsets.put(tp, lastRecOffset)
      }
    }
  }

  private def reachEndOffset(): Boolean = {
    targetOffsets.forall { case (partition, targetOffset) =>
      currentOffsets.get(partition) match {
        case Some(currentOffset) => currentOffset >= targetOffset
        case None                => false
      }
    }
  }
}
