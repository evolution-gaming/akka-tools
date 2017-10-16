package com.evolutiongaming.util

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try


/**
  * Runs functions asynchronously sequentially if key matches, or in parallel otherwise
  */
trait FutureSequentialForKey extends Extension {
  def apply[T](key: Any)(f: => T): Future[T]
}

class FutureSequentialForKeyImpl(factory: ActorRefFactory, name: Option[String])(implicit ec: ExecutionContext)
  extends FutureSequentialForKey {

  private implicit val timeout = Timeout(5.seconds)
  private lazy val supervisor = {
    val props = Supervisor.props
    name map { name => factory.actorOf(props, name) } getOrElse factory.actorOf(props)
  }
  private val refs = TrieMap.empty[Any, ActorRef]

  def apply[T](key: Any)(f: => T): Future[T] = {
    val enqueue = FutureSupervisor.Enqueue(() => f)
    val future = refs.get(key)
      .map { ref => ref ? enqueue }
      .getOrElse { supervisor ? Supervisor.Create(key, enqueue) }
    for {
      f1 <- future.mapTo[Future[T]]
      f2 <- f1
    } yield f2
  }

  private class Supervisor extends Actor {
    import Supervisor._

    def receive = receive(Map())

    def receive(keys: Map[ActorRef, Any]): Receive = {
      case Create(key, enqueue) => refs.get(key) match {
        case Some(ref) => ref.tell(enqueue, sender())
        case None      =>
          val ref = context watch (context actorOf FutureSupervisor.props)
          ref.tell(enqueue, sender())
          refs += (key -> ref)
          context become receive(keys + (ref -> key))
      }


      case Terminated(ref) =>
        for {key <- keys get ref} refs -= key
        context become receive(keys - ref)
    }
  }

  private object Supervisor {
    def props: Props = Props(new Supervisor)

    case class Create(key: Any, enqueue: FutureSupervisor.Enqueue)
  }


  private class FutureSupervisor extends Actor {
    import FutureSupervisor._

    context setReceiveTimeout 10.minutes

    def receive = receive(Future.successful(()))

    def receive(future: Future[Any]): Receive = {
      case Enqueue(func) =>
        val result = future recover { case _ => () } map { _ => func() }
        sender() ! result
        context become receive(result)

      case ReceiveTimeout =>
        context stop self
    }
  }

  private object FutureSupervisor {
    def props: Props = Props(new FutureSupervisor)

    case class Enqueue(func: () => Any)
  }
}

object FutureSequentialForKey extends ExtensionId[FutureSequentialForKey] {
  val Blocking: FutureSequentialForKey = new FutureSequentialForKey {
    def apply[T](key: Any)(f: => T) = Future fromTry Try(f)
  }

  def createExtension(system: ExtendedActorSystem) = {
    new FutureSequentialForKeyImpl(system, Some("FutureSequentialForKey"))(system.dispatcher)
  }
}