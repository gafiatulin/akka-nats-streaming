package akka.stream.alpakka.nats.scaladsl

import akka.NotUsed
import akka.stream.alpakka.nats.{IncomingMessage, NatsStreamingSimpleSourceStage, SimpleSubscriptionSettings}
import akka.stream.scaladsl.Source
import com.typesafe.config.Config

object NatsStreamingSimpleSource {
  def apply(settings: SimpleSubscriptionSettings): Source[IncomingMessage[Array[Byte]], NotUsed] =
    Source.fromGraph(new NatsStreamingSimpleSourceStage(settings))

  def apply(config: Config): Source[IncomingMessage[Array[Byte]], NotUsed] =
    apply(SimpleSubscriptionSettings.fromConfig(config))
}
