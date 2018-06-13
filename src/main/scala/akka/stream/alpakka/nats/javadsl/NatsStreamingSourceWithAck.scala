package akka.stream.alpakka.nats.javadsl

import akka.NotUsed
import akka.stream.alpakka.nats.{IncomingMessageWithAck, NatsStreamingSourceWithAckStage, SubscriptionWithAckSettings}
import akka.stream.javadsl.Source
import com.typesafe.config.Config

object NatsStreamingSourceWithAck {
  def create(settings: SubscriptionWithAckSettings): Source[IncomingMessageWithAck[Array[Byte]], NotUsed] =
    Source.fromGraph(new NatsStreamingSourceWithAckStage(settings))

  def create(config: Config): Source[IncomingMessageWithAck[Array[Byte]], NotUsed] =
    create(SubscriptionWithAckSettings.fromConfig(config))

}
