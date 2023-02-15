package akcl

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

import java.util

class CompositeRebalanceListener(a: ConsumerRebalanceListener, b: ConsumerRebalanceListener) extends ConsumerRebalanceListener {
  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
    a.onPartitionsRevoked(partitions)
    b.onPartitionsRevoked(partitions)
  }

  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
    a.onPartitionsAssigned(partitions)
    b.onPartitionsAssigned(partitions)
  }
}
