package akka.stream.alpakka.nats.scaladsl

import akka.NotUsed
import akka.stream.alpakka.nats.{IncomingMessageWithAck, NatsStreamingSourceWithAckStage, SubscriptionWithAckSettings}
import akka.stream.scaladsl.Source
import com.typesafe.config.Config

object NatsStreamingSourceWithAck {
  def apply(settings: SubscriptionWithAckSettings): Source[IncomingMessageWithAck[Array[Byte]], NotUsed] =
    Source.fromGraph(new NatsStreamingSourceWithAckStage(settings))

  def apply(config: Config): Source[IncomingMessageWithAck[Array[Byte]], NotUsed] =
    apply(SubscriptionWithAckSettings.fromConfig(config))

}
