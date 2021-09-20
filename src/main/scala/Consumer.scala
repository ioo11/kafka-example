import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties
import java.time.Duration
import java.util
import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.JavaConverters.{asJavaCollectionConverter, iterableAsScalaIterableConverter}

object Consumer extends App {
  val bookTopic = "book"
  val messageCount = 5
  val kafkaProps = initProps

  val consumer = new KafkaConsumer(kafkaProps, new StringDeserializer, new StringDeserializer)

  val topicPartitions = getTopicPartitions
  consumer.assign(topicPartitions)

  seekLastMessages()

  consumer
    .poll(Duration.ofSeconds(10))
    .asScala
    .foreach { r => println(r.value()) }

  consumer.close()

  def initProps = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:29092")
    props.put("group.id", "consumer1")
    props.put("max.poll.records", "15")
    props
  }

  def getTopicPartitions: util.Collection[TopicPartition] =
    consumer
      .partitionsFor(bookTopic).asScala
      .map(t => new TopicPartition(t.topic(), t.partition())).asJavaCollection

  def seekLastMessages(): Unit = {
    consumer.seekToEnd(topicPartitions)
    topicPartitions.foreach(partition => {
      consumer.seek(partition, getMessageOffset(consumer.position(partition)))
    })
  }

  def getMessageOffset(current: Long): Long = if (current < messageCount) 0L else current - messageCount
}
