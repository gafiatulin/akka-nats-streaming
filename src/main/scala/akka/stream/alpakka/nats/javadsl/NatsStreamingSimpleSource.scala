package akka.stream.alpakka.nats.javadsl

import akka.NotUsed
import akka.stream.alpakka.nats.{IncomingMessage, NatsStreamingSimpleSourceStage, SimpleSubscriptionSettings}
import akka.stream.javadsl.Source
import com.typesafe.config.Config

object NatsStreamingSimpleSource {
  def create(settings: SimpleSubscriptionSettings): Source[IncomingMessage[Array[Byte]], NotUsed] =
    Source.fromGraph(new NatsStreamingSimpleSourceStage(settings))

  def create(config: Config): Source[IncomingMessage[Array[Byte]], NotUsed] =
    create(SimpleSubscriptionSettings.fromConfig(config))
}
