package akcl

import cats.effect.{IO, Resource}
import io.github.embeddedkafka.{EmbeddedK, EmbeddedKafka}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory
import weaver._

import java.time.Duration
import java.util.{Collections, Properties}

object UpsertKafkaConsumerTest extends MutableIOSuite {
  val logger = LoggerFactory.getLogger(classOf[UpsertKafkaConsumerTest.type])

  override type Res = EmbeddedK

  override def sharedResource: Resource[IO, Res] = {
    Resource.make(
      IO {
        EmbeddedKafka.start()
      }
    )(s =>
      IO {
        s.stop(true)
        EmbeddedKafka.stop()
      }
    )
  }

  val testTopic = "helloworld"

  def testRecords(n: Int): Seq[(String, String)] = {
    for {
      i <- 0 until n
      j <- 0 until n
    } yield {
      i.toString -> j.toString
    }
  }

  test("1st poll should materialize all data into an upsert map") { em =>
    def produceTask(n: Int) = IO {
      val producerProps = new Properties()
      producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, s"127.0.0.1:${em.config.kafkaPort}")
      producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      implicit val sSer: StringSerializer = new org.apache.kafka.common.serialization.StringSerializer()
      val producer                        = new KafkaProducer[String, String](producerProps)
      for (rec <- testRecords(n)) {
        producer.send(new ProducerRecord(testTopic, rec._1, rec._2))
      }
      producer.flush()
      producer.close()
    }
    implicit val sDes: StringDeserializer = new org.apache.kafka.common.serialization.StringDeserializer
    for {
      _ <- produceTask(1000)
      consumer <- IO {
        val consumerProps = new Properties()
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, s"127.0.0.1:${em.config.kafkaPort}")
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "qqq")
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "qq")
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500")
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

        new UpsertKafkaConsumer[String, String](consumerProps)
      }
      parts <- IO {
        consumer.subscribe(Collections.singleton(testTopic))
        consumer.partitionsFor(testTopic)
      }
      rec1 <- IO(consumer.poll(Duration.ofMillis(5000)))
      rec2 <- IO(consumer.poll(Duration.ofMillis(5000)))
      rec3 <- IO(consumer.poll(Duration.ofMillis(5000)))
    } yield {
      assert.all(
        rec1.count() == 1000,
        rec2.count() == 0,
        rec3.count() == 0
      )
    }

  }

}
