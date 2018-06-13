package akka.stream.alpakka.nats.javadsl

import akka.Done
import akka.stream.alpakka.nats.{NatsStreamingSimpleSinkStage, OutgoingMessage, PublishingSettings}
import akka.stream.javadsl.Sink
import com.typesafe.config.Config

import scala.concurrent.Future

object NatsStreamingSimpleSink {
  def create(settings: PublishingSettings): Sink[OutgoingMessage[Array[Byte]], Future[Done]] =
    Sink.fromGraph(new NatsStreamingSimpleSinkStage(settings))

  def create(config: Config): Sink[OutgoingMessage[Array[Byte]], Future[Done]] =
    create(PublishingSettings.fromConfig(config))
}
