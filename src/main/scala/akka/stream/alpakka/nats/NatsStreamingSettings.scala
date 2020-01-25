package akka.stream.alpakka.nats

import java.time.{Duration, Instant}

import com.typesafe.config.Config
import io.nats.streaming.SubscriptionOptions

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

sealed trait DeliveryStartPosition

object DeliveryStartPosition{
  case object OnlyNew extends DeliveryStartPosition
  case object AllAvailable extends DeliveryStartPosition
  case object LastReceived extends DeliveryStartPosition
  case class AfterSequenceNumber(seq: Long) extends DeliveryStartPosition
  case class AfterTime(time: Instant) extends DeliveryStartPosition
  final def fromConfig(config: Config): DeliveryStartPosition = Try(config.getLong("deliver-after-sequence-number")) match {
    case Success(seq) => AfterSequenceNumber(seq)
    case Failure(_) =>
      Try(Instant.ofEpochMilli(config.getLong("deliver-after-epoch-millis")))
        .orElse(Try(Instant.parse(config.getString("deliver-after-instance")))) match {
        case Success(time) => AfterTime(time)
        case Failure(_) => config.getString("deliver").toLowerCase match {
          case "only-new" => OnlyNew
          case "all-available" => AllAvailable
          case "last-received" => LastReceived
        }
      }
  }
}

final case class NatsStreamingConnectionSettings(
  clusterId: String,
  clientId: String,
  url: String,
  connectionTimeout: Option[Duration],
  publishAckTimeout: Option[Duration],
  publishMaxInFlight: Option[Int],
  discoverPrefix: Option[String]
)

case object NatsStreamingConnectionSettings{
  private final def url(host: String, port: Int): String = "nats://" + host + ":" + port.toString
  final def fromConfig(config: Config): NatsStreamingConnectionSettings =
    NatsStreamingConnectionSettings(
      clusterId = config.getString("cluster-id"),
      clientId = config.getString("client-id"),
      url = Try(config.getString("url")).getOrElse(url(config.getString("host"), config.getInt("port"))),
      connectionTimeout = Try(config.getDuration("connection-timeout")).toOption,
      publishAckTimeout = Try(config.getDuration("pub-ack-timeout")).toOption,
      publishMaxInFlight = Try(config.getInt("pub-max-in-flight")).toOption,
      discoverPrefix = Try(config.getString("discover-prefix")).toOption
    )
}

sealed trait NatsStreamingSubscriptionSettings{
  def cp: StreamingConnectionProvider
  def subjects: List[String]
  def subscriptionQueue: String
  def durableSubscriptionName: Option[String]
  def startPosition: DeliveryStartPosition
  def subMaxInFlight: Option[Int]
  def bufferSize: Int
  def autoRequeueTimeout: Option[Duration]
  def manualAcks: Boolean
  def closeConnectionAfterStop: Boolean
  def subscriptionOptions: SubscriptionOptions = {
    val b = new SubscriptionOptions.Builder()
    val bMaxInFlight = subMaxInFlight.map(b.maxInFlight).getOrElse(b)
    val bAckWait = autoRequeueTimeout.map(bMaxInFlight.ackWait).getOrElse(bMaxInFlight)
    val bDurable = durableSubscriptionName.map(bAckWait.durableName).getOrElse(bAckWait)
    val builder = if(manualAcks) bDurable.manualAcks() else bDurable
    startPosition match {
      case DeliveryStartPosition.OnlyNew => builder
      case DeliveryStartPosition.AllAvailable => builder.deliverAllAvailable
      case DeliveryStartPosition.LastReceived => builder.startWithLastReceived
      case DeliveryStartPosition.AfterSequenceNumber(seq) => builder.startAtSequence(seq)
      case DeliveryStartPosition.AfterTime(time) => builder.startAtTime(time)
    }
  }.build()
}

case object NatsStreamingSubscriptionSettings{
  val defaultBufferSize = 1024
}

final case class SimpleSubscriptionSettings(
  cp: StreamingConnectionProvider,
  subjects: List[String],
  subscriptionQueue: String,
  durableSubscriptionName: Option[String],
  startPosition: DeliveryStartPosition,
  subMaxInFlight: Option[Int],
  bufferSize: Int,
  autoRequeueTimeout: Option[Duration],
  manualAcks: Boolean,
  closeConnectionAfterStop: Boolean
) extends NatsStreamingSubscriptionSettings

case object SimpleSubscriptionSettings{
  def fromConfig(config: Config, connectionProvider: StreamingConnectionProvider): SimpleSubscriptionSettings =
    SimpleSubscriptionSettings(
      cp = connectionProvider,
      subjects = Try(config.getStringList("subjects").asScala.toList)
        .orElse(Try(config.getString("subjects").split(',').map(_.trim).toList))
        .orElse(Try(config.getStringList("subject").asScala.toList))
        .getOrElse(config.getString("subject").split(',').map(_.trim).toList),
      subscriptionQueue = config.getString("subscription-queue"),
      durableSubscriptionName = Try(config.getString("durable-subscription-name")).toOption,
      startPosition = Try(DeliveryStartPosition.fromConfig(config)).getOrElse(DeliveryStartPosition.OnlyNew),
      subMaxInFlight = Try(config.getInt("subscription-max-in-flight")).toOption,
      bufferSize = Try(config.getInt("buffer-size")).getOrElse(NatsStreamingSubscriptionSettings.defaultBufferSize),
      autoRequeueTimeout = Try(config.getDuration("auto-requeue-timeout")).toOption,
      manualAcks = Try(config.getBoolean("manual-acks")).getOrElse(true),
      closeConnectionAfterStop = Try(config.getBoolean("close-connection-after-stop")).getOrElse(true)
    )
  def fromConfig(config: Config): SimpleSubscriptionSettings = fromConfig(config, NatsStreamingConnectionBuilder.fromConfig(config))
}

final case class SubscriptionWithAckSettings(
  cp: StreamingConnectionProvider,
  subjects: List[String],
  subscriptionQueue: String,
  durableSubscriptionName: Option[String],
  startPosition: DeliveryStartPosition,
  subMaxInFlight: Option[Int],
  manualAckTimeout: Duration,
  autoRequeueTimeout: Option[Duration],
  bufferSize: Int,
  manualAcks: Boolean,
  closeConnectionAfterStop: Boolean
) extends NatsStreamingSubscriptionSettings

case object SubscriptionWithAckSettings{
  def fromConfig(config: Config, connectionProvider: StreamingConnectionProvider): SubscriptionWithAckSettings = {
    val simple = SimpleSubscriptionSettings.fromConfig(config, connectionProvider)
    SubscriptionWithAckSettings(
      simple.cp,
      simple.subjects,
      simple.subscriptionQueue,
      simple.durableSubscriptionName,
      simple.startPosition,
      simple.subMaxInFlight,
      config.getDuration("manual-ack-timeout"),
      Some(config.getDuration("auto-requeue-timeout")),
      simple.bufferSize,
      manualAcks = true,
      simple.closeConnectionAfterStop
    )
  }
  def fromConfig(config: Config): SubscriptionWithAckSettings =
    fromConfig(config, NatsStreamingConnectionBuilder.fromConfig(config))
}

final case class PublishingSettings(
  cp: StreamingConnectionProvider,
  defaultSubject: String,
  parallel: Boolean,
  closeConnectionAfterStop: Boolean
)

case object PublishingSettings{
  def fromConfig(config: Config): PublishingSettings =
    PublishingSettings(
      cp = NatsStreamingConnectionBuilder.fromConfig(config),
      defaultSubject = config.getString("default-subject"),
      parallel = config.getBoolean("parallel"),
      closeConnectionAfterStop = Try(config.getBoolean("close-connection-after-stop")).getOrElse(true)
    )
}
