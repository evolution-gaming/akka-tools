package com.evolutiongaming.cluster

import akka.actor.{ActorRef, Extension}

class ActorRefExtension(val ref: ActorRef) extends Extension