package akka.stream.alpakka.nats.scaladsl

import akka.Done
import akka.stream.alpakka.nats.{NatsStreamingSimpleSinkStage, OutgoingMessage, PublishingSettings}
import akka.stream.scaladsl.Sink
import com.typesafe.config.Config

import scala.concurrent.Future

object NatsStreamingSimpleSink {
  def apply(settings: PublishingSettings): Sink[OutgoingMessage[Array[Byte]], Future[Done]] =
    Sink.fromGraph(new NatsStreamingSimpleSinkStage(settings))

  def apply(config: Config): Sink[OutgoingMessage[Array[Byte]], Future[Done]] =
    apply(PublishingSettings.fromConfig(config))
}
