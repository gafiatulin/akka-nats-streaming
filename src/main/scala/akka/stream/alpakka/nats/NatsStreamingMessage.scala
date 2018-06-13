package akka.stream.alpakka.nats

import akka.Done

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

sealed trait NatsStreamingMessage[T]{
  def data: T
  def transform[T2](f: T => T2): NatsStreamingMessage[T2]
  def subject: Option[String]
}

sealed trait NatsStreamingIncoming[T] extends NatsStreamingMessage[T]
sealed trait NatsStreamingOutgoing[T] extends NatsStreamingMessage[T]

case class IncomingMessage[T](data: T, subject: Option[String] = None) extends NatsStreamingIncoming[T]{
  def transform[T2](f: T => T2): IncomingMessage[T2] =
    IncomingMessage(f(data), subject)
}

case class IncomingMessageWithAck[T](data: T, subject: Option[String] = None) extends NatsStreamingIncoming[T]{
  private[nats] val promise =
    Promise[Done]
  def transform[T2](f: T => T2): IncomingMessageWithAck[T2] =
    IncomingMessageWithAck(f(data), subject, promise)
  def ack: Try[T] =
    if(promise.trySuccess(Done)) Success(data) else Failure(new Exception("Already completed"))
}

object IncomingMessageWithAck{
  private[nats] def apply[T](data: T, subject: Option[String], p: Promise[Done]): IncomingMessageWithAck[T] =
    new IncomingMessageWithAck(data, subject){
      override val promise: Promise[Done] = p
    }
}

case class OutgoingMessage[T](data: T, subject: Option[String] = None) extends NatsStreamingOutgoing[T]{
  def transform[T2](f: T => T2): OutgoingMessage[T2] =
    OutgoingMessage(f(data), subject)
}

case class OutgoingMessageWithCompletion[T](data: T, subject: Option[String] = None) extends NatsStreamingOutgoing[T]{
  private[nats] val promise = Promise[Done]
  def transform[T2](f: T => T2): OutgoingMessageWithCompletion[T2] =
    OutgoingMessageWithCompletion(f(data), subject, promise)
  def completion: Future[Done] =
    promise.future
}

object OutgoingMessageWithCompletion{
  private[nats] def apply[T](data: T, subject: Option[String], p: Promise[Done]): OutgoingMessageWithCompletion[T] =
    new OutgoingMessageWithCompletion(data, subject){
      override val promise: Promise[Done] = p
    }
}
