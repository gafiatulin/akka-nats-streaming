package akka.stream.alpakka.nats.javadsl

import akka.Done
import akka.stream.alpakka.nats.{NatsStreamingSinkWithCompletionStage, OutgoingMessageWithCompletion, PublishingSettings}
import akka.stream.javadsl.Sink
import com.typesafe.config.Config

import scala.concurrent.Future

object NatsStreamingSinkWithCompletion {
  def create(settings: PublishingSettings): Sink[OutgoingMessageWithCompletion[Array[Byte]], Future[Done]] =
    Sink.fromGraph(new NatsStreamingSinkWithCompletionStage(settings))

  def create(config: Config): Sink[OutgoingMessageWithCompletion[Array[Byte]], Future[Done]] =
    create(PublishingSettings.fromConfig(config))
}
