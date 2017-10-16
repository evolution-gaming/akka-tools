package akka.persistence

object JournalFailure {
  def unapply(any: Any): Option[JournalProtocol.Response] = PartialFunction.condOpt(any) {
    case x: JournalProtocol.WriteMessageFailure  => x
    case x: JournalProtocol.WriteMessageRejected => x
  }
}

object SnapshotFailure {
  def unapply(any: Any): Option[SnapshotProtocol.Response] = PartialFunction.condOpt(any) {
    case x: SaveSnapshotFailure    => x
    case x: DeleteSnapshotFailure  => x
    case x: DeleteSnapshotsFailure => x
  }
}
