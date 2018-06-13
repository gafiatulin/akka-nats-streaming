package akka.stream.alpakka.nats

import com.typesafe.config.Config
import io.nats.streaming.{NatsStreaming, Options, StreamingConnection}

trait StreamingConnectionProvider {
  def connection: StreamingConnection
}

final case class NatsStreamingConnectionBuilder(clusterId: String, clientId: String, options: Options) extends StreamingConnectionProvider{
  def connection: StreamingConnection = NatsStreaming.connect(clusterId, clientId, options)
}

object NatsStreamingConnectionBuilder{
  def fromConfig(config: Config): NatsStreamingConnectionBuilder = fromSettings(NatsStreamingConnectionSettings.fromConfig(config))
  def fromSettings(settings: NatsStreamingConnectionSettings): NatsStreamingConnectionBuilder = {
    val b = new Options.Builder().natsUrl(settings.url)
    val bConT = settings.connectionTimeout.map(b.connectWait).getOrElse(b)
    val bPubAckT = settings.publishAckTimeout.map(bConT.pubAckWait).getOrElse(bConT)
    val bMaxInF = settings.publishMaxInFlight.map(bPubAckT.maxPubAcksInFlight).getOrElse(bPubAckT)
    val options = settings.discoverPrefix.map(bMaxInF.discoverPrefix).getOrElse(bMaxInF).build()
    NatsStreamingConnectionBuilder(settings.clusterId, settings.clientId, options)
  }
}
