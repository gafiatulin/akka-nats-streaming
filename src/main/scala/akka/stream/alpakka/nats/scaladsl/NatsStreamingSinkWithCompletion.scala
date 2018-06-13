package akka.stream.alpakka.nats.scaladsl

import akka.Done
import akka.stream.alpakka.nats.{NatsStreamingSinkWithCompletionStage, OutgoingMessageWithCompletion, PublishingSettings}
import akka.stream.scaladsl.Sink
import com.typesafe.config.Config

import scala.concurrent.Future

object NatsStreamingSinkWithCompletion {
  def apply(settings: PublishingSettings): Sink[OutgoingMessageWithCompletion[Array[Byte]], Future[Done]] =
    Sink.fromGraph(new NatsStreamingSinkWithCompletionStage(settings))

  def apply(config: Config): Sink[OutgoingMessageWithCompletion[Array[Byte]], Future[Done]] =
    apply(PublishingSettings.fromConfig(config))
}
