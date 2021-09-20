import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.apache.commons.csv.{CSVFormat, CSVRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.io.FileReader
import java.util
import java.util.Properties
import scala.collection.JavaConversions.collectionAsScalaIterable


object Producer extends App {
  val data = getCsvFromPath("src/main/resources/bestsellers with categories.csv")
  val kafkaProps = initProps

  val producer = new KafkaProducer(kafkaProps, new StringSerializer, new StringSerializer)
  val mapper = initMapper

  data
    .map(r => toBook(r))
    .foreach { book => {
      val rec = new ProducerRecord("book", book.name, mapper.writeValueAsString(book))
      producer send rec
    }}

  producer.close()


  def initProps = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:29092")
    props.put("group.id", "consumer1")
    props
  }

  def initMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper
  }

  def toBook(record: CSVRecord): Book = Book(record.get("Name"),
    record.get("Author"),
    record.get("User Rating"),
    record.get("Reviews"),
    record.get("Price"),
    record.get("Year"),
    record.get("Genre")
  )

  private def getCsvFromPath(fileName: String): util.List[CSVRecord] = {
    CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new FileReader(fileName)).getRecords
  }
}
