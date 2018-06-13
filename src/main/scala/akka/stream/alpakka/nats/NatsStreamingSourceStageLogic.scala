package akka.stream.alpakka.nats

import java.util.concurrent.TimeUnit

import akka.Done
import akka.stream.impl.Buffer
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import io.nats.client.{ConnectionEvent, NATSException}
import io.nats.streaming.{Message, MessageHandler, StreamingConnection}

import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

private[nats] abstract class NatsStreamingSourceStageLogic[T1 <: NatsStreamingSubscriptionSettings, T2 <: NatsStreamingIncoming[Array[Byte]]](
  settings: T1,
  shape: SourceShape[T2],
  out: Outlet[T2]
) extends GraphStageLogic(shape) with OutHandler with StageLogging{
  private final var downstreamWaiting = false
  private final var closed = false
  private final var subscriptions: Seq[io.nats.streaming.Subscription] = Seq.empty
  private final var connection: StreamingConnection = _
  private final var failureLogic: AsyncCallback[Throwable] = _
  protected final var buffer: Buffer[T2] = _
  protected final var processingLogic: AsyncCallback[Unit] = _
  protected final val scheduled = new java.util.concurrent.atomic.AtomicBoolean(false)
  protected val messageHandler: MessageHandler

  private final def cleanup(): Unit = if(settings.closeConnectionAfterStop){
    if(null != connection && !closed) try{
      connection.close()
      closed = true
    } catch {
      case NonFatal(e) =>
        log.error(e, "Failed to close nats connection")
        cleanup()
    }
  } else {
    subscriptions.foreach(_.close())
  }

  private final def handleFailure(e: Throwable): Unit = {
    log.error(e, "Caught Exception. Failing stage...")
    failStage(e)
  }

  private final def process(u: Unit): Unit = {
    if (!scheduled.compareAndSet(true, false)) throw new IllegalStateException("Code should never reach here")
    if (downstreamWaiting && (!buffer.isEmpty)) {
      val e = buffer.dequeue()
      if(null != e){
        downstreamWaiting = false
        push(out, e)
      }
    }
    u
  }

  override def preStart(): Unit = try{
    buffer = Buffer[T2](settings.bufferSize, materializer)
    failureLogic = getAsyncCallback(handleFailure)
    processingLogic = getAsyncCallback(process)
    connection = settings.cp.connection
    subscriptions = settings.subjects.map{s =>
      connection.subscribe(s, settings.subscriptionQueue, messageHandler, settings.subscriptionOptions)
    }
    val natsConnection = connection.getNatsConnection
    natsConnection.setClosedCallback((_: ConnectionEvent) => failureLogic.invoke(new Exception("Connection closed")))
    natsConnection.setDisconnectedCallback((_: ConnectionEvent) => failureLogic.invoke(new Exception("Disconnected")))
    natsConnection.setExceptionHandler((e: NATSException) => failureLogic.invoke(e))
    if (scheduled.compareAndSet(false, true)) processingLogic.invoke(())
    log.debug("Nats connection initiated")
    super.preStart()
  } catch{
    case NonFatal(e) =>
      handleFailure(e)
  }

  override def postStop(): Unit = {
    try{
      cleanup()
    } catch {
      case NonFatal(e) =>
        log.error(e, "Exception during cleanup")
    }
    super.postStop()
  }

  override def onPull(): Unit = if (buffer.isEmpty) {
    downstreamWaiting = true
  } else {
    val e = buffer.dequeue()
    if(null == e) {
      downstreamWaiting = true
    } else {
      push(out, e)
    }
  }
  setHandler(out, this)
}

private[nats] class NatsStreamingSimpleSourceStageLogic(
  settings: SimpleSubscriptionSettings,
  shape: SourceShape[IncomingMessage[Array[Byte]]],
  out: Outlet[IncomingMessage[Array[Byte]]]
) extends NatsStreamingSourceStageLogic(settings, shape, out){
  val messageHandler: MessageHandler = (msg: Message) => {
    buffer.enqueue(IncomingMessage(msg.getData, Option(msg.getSubject)))
    if (settings.manualAcks) msg.ack()
    if (scheduled.compareAndSet(false, true)) processingLogic.invoke(())
  }
}

private[nats] class NatsStreamingSourceWithAckStageLogic(
  settings: SubscriptionWithAckSettings,
  shape: SourceShape[IncomingMessageWithAck[Array[Byte]]],
  out: Outlet[IncomingMessageWithAck[Array[Byte]]]
) extends NatsStreamingSourceStageLogic(settings, shape, out){
  val messageHandler: MessageHandler = (msg: Message) => {
    val promise = Promise[Done]()
    buffer.enqueue(IncomingMessageWithAck(msg.getData, Option(msg.getSubject), promise))
    if (scheduled.compareAndSet(false, true)) processingLogic.invoke(())
    val cancelable = materializer.scheduleOnce(FiniteDuration(settings.manualAckTimeout.toNanos, TimeUnit.NANOSECONDS), () => {
      promise.tryFailure(new Exception(s"Didn't process message during ${settings.manualAckTimeout}"))
      ()
    })
    promise.future.foreach { _ =>
      msg.ack()
      cancelable.cancel()
    }(materializer.executionContext)
  }
}

class NatsStreamingSimpleSourceStage(settings: SimpleSubscriptionSettings)
  extends GraphStage[SourceShape[IncomingMessage[Array[Byte]]]]{
  val out: Outlet[IncomingMessage[Array[Byte]]] = Outlet("NatsStreamingSimpleSource.out")
  val shape: SourceShape[IncomingMessage[Array[Byte]]] = SourceShape(out)
  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new NatsStreamingSimpleSourceStageLogic(settings, shape, out)
}

class NatsStreamingSourceWithAckStage(settings: SubscriptionWithAckSettings)
  extends GraphStage[SourceShape[IncomingMessageWithAck[Array[Byte]]]]
{
  require(settings.manualAckTimeout.compareTo(settings.autoRequeueTimeout.get) <= 0)
  val out: Outlet[IncomingMessageWithAck[Array[Byte]]] = Outlet("NatsStreamingSourceWithAck.out")
  val shape: SourceShape[IncomingMessageWithAck[Array[Byte]]] = SourceShape(out)
  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new NatsStreamingSourceWithAckStageLogic(settings, shape, out)
}
