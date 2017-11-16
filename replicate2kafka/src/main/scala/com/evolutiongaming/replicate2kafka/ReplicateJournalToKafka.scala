package com.evolutiongaming.replicate2kafka

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Producer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.{Serialization, SerializationExtension}
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.persistence.PersistenceId
import com.evolutiongaming.util.ConfigHelper._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringSerializer, Serializer => KafkaSerializer}

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Success, Try}

trait ReplicateJournalToKafka {
  def apply(messages: Seq[AtomicWrite], result: ReplicateJournalToKafka.Result): ReplicateJournalToKafka.Result
}

object ReplicateJournalToKafka extends LazyLogging {
  
  type Result = Future[Seq[Try[Unit]]]
  type ProducerMsg = ProducerMessage.Message[String, Elem, Elem]

  def apply(system: ActorSystem, replicateTypes: List[String]): ReplicateJournalToKafka = {

    val config = system.settings.config

    val replicateToKafkaConfig = config.getConfig("evolutiongaming.journal.replicate-to-kafka")
    val enabled = replicateToKafkaConfig.getBoolean("enabled")
    val bufferSize = replicateToKafkaConfig.getInt("bufferSize")
    val overflowStrategy = replicateToKafkaConfig.get[OverflowStrategy]("overflowStrategy")

    def impl: ReplicateJournalToKafka = {

      def sink = {
        val settings = ProducerSettings[String, Elem](
          replicateToKafkaConfig withFallback config.getConfig("evolutiongaming.kafka-sender"),
          new StringSerializer,
          new ElemSerializer(SerializationExtension(system)))
        Producer
          .flow[String, Elem, Elem](settings)
          .toMat(Sink.ignore)(Keep.right)
      }

      def resume(failure: Throwable): Supervision.Directive = {
        logger.error(s"kafka stream failure $failure", failure)
        Supervision.Resume
      }

      def materializer = ActorMaterializer(
        ActorMaterializerSettings(system).withSupervisionStrategy(resume _),
        "kafka.core.journal")(system)

      new Impl(
        replicateTypes,
        bufferSize = bufferSize,
        overflowStrategy = overflowStrategy,
        SerializationExtension(system),
        materializer,
        sink,
        system.dispatcher)
    }
    
    if (enabled) impl else Empty
  }


  object Empty extends ReplicateJournalToKafka {
    def apply(messages: Seq[AtomicWrite], result: Result): Result = result
  }

  class Impl(
    replicateTypes: List[String],
    bufferSize: Int,
    overflowStrategy: OverflowStrategy,
    serialization: Serialization,
    materializer: => Materializer,
    sink: => Sink[ProducerMsg, Future[Done]],
    private implicit val executor: ExecutionContext) extends ReplicateJournalToKafka {

    private lazy val kafka: SourceQueueWithComplete[Elem] = {

      def toProducerRecord(elem: Elem): ProducerRecord[String, Elem] = {
        val payload = elem.payload
        val persistenceId = payload.persistenceId
        val topic = s"core.journal.${ elem.persistenceType }"
        
        logger.debug(s"persistenceId: $persistenceId topic: $topic")
        new ProducerRecord(topic, persistenceId, elem)
      }

      val (source, done) = Source
        .queue[Elem](bufferSize, overflowStrategy)
        .map(x => ProducerMessage.Message(toProducerRecord(x), x))
        .toMat(sink)(Keep.both)
        .run()(materializer)

      for {failure <- done.failed} logger.error(s"kafka stream failed $failure", failure)

      source
    }

    def apply(messages: Seq[AtomicWrite], result: Result): Result = {
      messages.headOption match {
        case None              => result
        case Some(atomicWrite) => result.andThen { case Success(result) if result forall { _.isSuccess } =>
          for {
            (persistenceType, _) <- PersistenceId.unapply(atomicWrite.persistenceId)
            types <- replicateTypes
            if types contains persistenceType
            atomicWrite <- messages
            persistentRepr <- atomicWrite.payload
          } {

            val elem = Elem(persistenceType, persistentRepr)

            def errorMsg = s"failed to enqueue ${ elem.name }"

            val future = kafka offer elem
            future foreach {
              case QueueOfferResult.Enqueued         => logger.debug(s"enqueued ${ elem.name }")
              case QueueOfferResult.Dropped          => logger.error(s"$errorMsg: Dropped")
              case QueueOfferResult.Failure(failure) => logger.error(s"$errorMsg: Failure", failure)
              case QueueOfferResult.QueueClosed      => logger.error(s"$errorMsg: QueueClosed")
            }

            future.failed foreach { failure => logger.error(errorMsg, failure) }
          }
        }(CurrentThreadExecutionContext)
      }
    }
  }

  case class Elem(persistenceType: String, payload: PersistentRepr) {

    def persistenceId: String = payload.persistenceId

    def name: String = {
      val sequenceNr = payload.sequenceNr
      s"$persistenceId($sequenceNr)"
    }
  }

  implicit val overflowStrategyFromConf: FromConf[OverflowStrategy] = new FromConf[OverflowStrategy] {
    def apply(config: Config, path: String): OverflowStrategy = {
      (config getString path).toLowerCase match {
        case "drophead"     => OverflowStrategy.dropHead
        case "droptail"     => OverflowStrategy.dropTail
        case "dropbuffer"   => OverflowStrategy.dropBuffer
        case "dropnew"      => OverflowStrategy.dropNew
        case "backpressure" => OverflowStrategy.backpressure
        case "fail"         => OverflowStrategy.fail
        case x              => sys error s"no OverflowStrategy found for $x "
      }
    }
  }

  class ElemSerializer(serialization: Serialization) extends KafkaSerializer[Elem] {

    def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

    def serialize(topic: String, elem: Elem): Array[Byte] = {
      val payload = elem.payload
      try serialization.serialize(payload).get catch {
        case NonFatal(e) => throw new RuntimeException(s"failed to serialize $payload", e)
      }
    }

    def close(): Unit = ()
  }
}