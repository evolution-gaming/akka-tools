package com.evolutiongaming.replicate2kafka

import akka.kafka.ProducerMessage.Message
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.Serialization
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, OverflowStrategy}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.replicate2kafka.ReplicateJournalToKafka._
import com.evolutiongaming.test.ActorSpec
import org.apache.kafka.clients.producer.ProducerRecord
import org.mockito.Mockito._
import org.mockito.{ArgumentMatchers => M}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

class ReplicateJournalToKafkaSpec extends WordSpec with Matchers with ActorSpec with MockitoSugar {

  "ReplicateJournalToKafka" should {

    "replicate events to kafka" in new Scope {
      val messages = List(AtomicWrite(List(persistentRepr(event1), persistentRepr(event2))))
      replicateJournalToKafka(messages, result)
      expectMsg(message(event1))
      expectMsg(message(event2))
    }

    "not replicate if type is not defined in settings" in new Scope {
      val messages = List(AtomicWrite(List(persistentRepr(Event(1, "Unknown-Id")))))
      replicateJournalToKafka(messages, result)
      expectNoMessage(10.millis)
    }

    "not replicate if messages are empty" in new Scope {
      replicateJournalToKafka(Nil, result)
      expectNoMessage(10.millis)
    }

    "not replicate if persistence failed" in new Scope {
      val messages = List(AtomicWrite(List(persistentRepr(event1))))
      replicateJournalToKafka(messages, Future.failed(new RuntimeException))
      expectNoMessage(10.millis)
    }
  }

  private class Scope extends ActorScope {
    val persistEntities = Set("GameTable")
    
    val materializer = ActorMaterializer()(system)
    val bytes = Array.empty[Byte]
    val serialization = {
      val serialization = mock[Serialization]
      when(serialization.serialize(M.any[AnyRef])) thenReturn Try(bytes)
      serialization
    }
    val sink = Sink.foreach[ProducerMsg](testActor ! _)

    val event1 = Event(1)
    val event2 = Event(2)

    val result: Result = Future.successful(Nil)

    val replicateJournalToKafka = new ReplicateJournalToKafka.Impl(
      persistEntities,
      bufferSize = 5,
      overflowStrategy = OverflowStrategy.fail,
      serialization = serialization,
      materializer = materializer,
      sink = sink,
      executor = CurrentThreadExecutionContext)

    def persistentRepr(x: Event) = PersistentRepr(payload = x, sequenceNr = x.sequenceNr, persistenceId = x.persistenceId)

    def message(x: Event): Message[String, Elem, Elem] = {
      val elem = Elem("GameTable", persistentRepr(x))
      val producerRecord = new ProducerRecord(s"core.journal.GameTable", x.persistenceId, elem)
      Message(producerRecord, elem)
    }

    case class Event(sequenceNr: Long, persistenceId: String = "GameTable-Id")
  }
}