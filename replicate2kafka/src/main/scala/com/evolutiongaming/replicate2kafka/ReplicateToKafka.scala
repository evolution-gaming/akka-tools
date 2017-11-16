package com.evolutiongaming.replicate2kafka

import akka.persistence.AtomicWrite
import akka.persistence.journal.AsyncWriteJournal
import com.evolutiongaming.replicate2kafka.ReplicateJournalToKafka.Result
import com.typesafe.scalalogging.LazyLogging
import com.evolutiongaming.util.Validation._

import scala.collection.immutable.Seq

trait ReplicateToKafka extends AsyncWriteJournal with LazyLogging {
  private lazy val replicateJournalToKafka = {
    ReplicateToKafkaExt(context.system).replicateJournalToKafka.orError()
  }

  abstract override def asyncWriteMessages(messages: Seq[AtomicWrite]): Result = {
    val result = super.asyncWriteMessages(messages)
    replicateJournalToKafka(messages, result)
  }
}