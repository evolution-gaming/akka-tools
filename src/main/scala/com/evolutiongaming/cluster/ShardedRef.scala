package com.evolutiongaming.cluster

import akka.actor.ActorRef
import akka.cluster.sharding.ShardRegion.ShardId

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag


trait ShardedRef[Id, In <: Serializable, Out] {

  def tell(id: Id, in: In, sender: ActorRef = ActorRef.noSender): Unit

  def ask(id: Id, in: In)(implicit timeout: FiniteDuration): Future[Out]

  def tellUnsafe(id: Id, in: Serializable, sender: ActorRef = ActorRef.noSender): Unit

  def askUnsafe(id: Id, in: Serializable)(implicit timeout: FiniteDuration): Future[Any]
}

object ShardedRef {

  def apply[Id, In <: Serializable, Out](
    ref: ActorRef,
    toShardId: Id => ShardId = (id: Id) => id.toString)
    (implicit tag: ClassTag[Out]): ShardedRef[Id, In, Out] = new Impl(ref, toShardId)


  abstract class SafeImpl[Id, In <: Serializable, Out](implicit tag: ClassTag[Out])
    extends ShardedRef[Id, In, Out] {

    def tell(id: Id, in: In, sender: ActorRef = ActorRef.noSender): Unit = {
      tellUnsafe(id, in, sender)
    }

    def ask(id: Id, in: In)(implicit timeout: FiniteDuration): Future[Out] = {
      askUnsafe(id, in).mapTo[Out]
    }
  }


  class Impl[Id, In <: Serializable, Out](
    ref: ActorRef,
    toShardId: Id => ShardId = (id: Id) => id.toString)
    (implicit tag: ClassTag[Out]) extends SafeImpl[Id, In, Out] {

    def askUnsafe(id: Id, in: Serializable)(implicit timeout: FiniteDuration): Future[Any] = {
      val shardedMsg = toShardedMsg(id, in)
      akka.pattern.ask(ref, shardedMsg)(timeout)
    }

    def tellUnsafe(id: Id, in: Serializable, sender: ActorRef = ActorRef.noSender): Unit = {
      val shardedMsg = toShardedMsg(id, in)
      ref.tell(shardedMsg, sender)
    }

    def toShardedMsg(id: Id, in: Serializable) = ShardedMsg(toShardId(id), in)

    override def toString = s"ShardedRef.Impl($ref)"
  }


  class Proxy[Id, In <: Serializable, Out](ref: ActorRef)(implicit tag: ClassTag[Out])
    extends SafeImpl[Id, In, Out] {

    def tellUnsafe(id: Id, in: Serializable, sender: ActorRef = ActorRef.noSender): Unit = {
      ref.tell(in, sender)
    }

    def askUnsafe(id: Id, in: Serializable)(implicit timeout: FiniteDuration): Future[Any] = {
      akka.pattern.ask(ref, in)(timeout)
    }

    override def toString = s"ShardedRef.Proxy($ref)"
  }

  object Proxy {
    def apply[Id, In <: Serializable, Out](ref: ActorRef)
      (implicit tag: ClassTag[Out]): ShardedRef[Id, In, Out] = new Proxy(ref)
  }
}


trait ShardedSingletonRef[In <: Serializable, Out] {

  def tell(in: In, sender: ActorRef = ActorRef.noSender): Unit

  def ask(in: In)(implicit timeout: FiniteDuration): Future[Out]

  def tellUnsafe(in: Serializable, sender: ActorRef = ActorRef.noSender): Unit

  def askUnsafe(in: Serializable)(implicit timeout: FiniteDuration): Future[Any]
}

object ShardedSingletonRef {

  def apply[Id, In <: Serializable, Out](id: Id, ref: ShardedRef[Id, In, Out]): ShardedSingletonRef[In, Out] = {
    new Impl(id, ref)
  }

  def apply[Id, In <: Serializable, Out](id: Id, ref: ActorRef)(implicit tag: ClassTag[Out]): ShardedSingletonRef[In, Out] = {
    val shardedRef = ShardedRef[Id, In, Out](ref)
    ShardedSingletonRef(id, shardedRef)
  }

  class Impl[Id, In <: Serializable, Out](id: Id, ref: ShardedRef[Id, In, Out]) extends ShardedSingletonRef[In, Out] {

    def tell(in: In, sender: ActorRef = ActorRef.noSender): Unit = {
      ref.tell(id, in, sender)
    }

    def ask(in: In)(implicit timeout: FiniteDuration): Future[Out] = {
      ref.ask(id, in)
    }

    def tellUnsafe(in: Serializable, sender: ActorRef = ActorRef.noSender): Unit = {
      ref.tellUnsafe(id, in, sender)
    }

    def askUnsafe(in: Serializable)(implicit timeout: FiniteDuration): Future[Any] = {
      ref.askUnsafe(id, in)
    }

    override def toString = s"ShardedSingletonRef.Impl($id)"
  }
}