package akka.stream.alpakka.nats

import akka.Done
import akka.stream.stage._
import akka.stream.{Attributes, Inlet, SinkShape}
import io.nats.client.{Connection, ConnectionListener, Consumer, ErrorListener}
import io.nats.streaming.{AckHandler, StreamingConnection}

import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

private[nats] abstract class NatsStreamingSinkStageLogic[T <: NatsStreamingOutgoing[Array[Byte]]](
  settings: PublishingSettings,
  promise: Promise[Done],
  shape: SinkShape[T],
  in: Inlet[T]
) extends GraphStageLogic(shape) with StageLogging{
  protected val successCallback: AsyncCallback[String] = getAsyncCallback(handleSuccess)
  protected val failureCallback: AsyncCallback[Throwable] = getAsyncCallback(handleFailure)
  private var connection: StreamingConnection = _
  def ah(m: T): AckHandler

  override def preStart(): Unit =
    try{
      val connectionListener: ConnectionListener = (_: Connection, `type`: ConnectionListener.Events) => `type` match {
        case ConnectionListener.Events.CLOSED => failureCallback.invoke(new Exception("Connection closed"))
        case ConnectionListener.Events.DISCONNECTED => failureCallback.invoke(new Exception("Disconnected"))
        case _ => ()
      }
      val errorListener: ErrorListener = new ErrorListener {
        def errorOccurred(conn: Connection, error: String): Unit = failureCallback.invoke(new Exception(error))
        def exceptionOccurred(conn: Connection, exp: Exception): Unit = log.debug("Nats exception occurred (handled by the library)", exp)
        def slowConsumerDetected(conn: Connection, consumer: Consumer): Unit = log.debug("Slow nats consumer detected")
      }
      connection = settings.cp.connection(connectionListener, errorListener)
      pull(in)
      super.preStart()
    } catch {
      case NonFatal(e) =>
        failureCallback.invoke(e)
    }

  override def postStop(): Unit = {
    if(settings.closeConnectionAfterStop){
      try{
        connection.close()
      } catch {
        case NonFatal(e) =>
          log.error(e, "Exception during nats connection close")
      }
    }
    promise.tryFailure(new RuntimeException("stage stopped unexpectedly"))
    super.postStop()
  }

  def handleFailure(ex: Throwable): Unit = {
    log.error(ex, "Caught Exception. Failing stage...")
    promise.tryFailure(ex)
    failStage(ex)
  }

  def handleSuccess(nuid: String): Unit = {
    log.debug("Successfully pushed {}", nuid)
    if(settings.parallel) () else pull(in)
  }

  setHandler(in, new InHandler {
    override def onUpstreamFailure(ex: Throwable): Unit = {
      promise.tryFailure(ex)
      super.onUpstreamFailure(ex)
    }

    override def onUpstreamFinish(): Unit = {
      promise.trySuccess(Done)
      super.onUpstreamFinish()
    }
    override def onPush(): Unit = {
      val m = grab(in)
      connection.publish(m.subject.getOrElse(settings.defaultSubject), m.data, ah(m))
      if(settings.parallel) pull(in) else ()
    }
  })
}

private[nats] class NatsStreamingSimpleSinkStageLogic(
  settings: PublishingSettings,
  promise: Promise[Done],
  shape: SinkShape[OutgoingMessage[Array[Byte]]],
  in: Inlet[OutgoingMessage[Array[Byte]]]
) extends NatsStreamingSinkStageLogic(settings, promise, shape, in){
  def ah(m: OutgoingMessage[Array[Byte]]): AckHandler =
    (nuid: String, ex: Exception) => if (Option(ex).isDefined) failureCallback.invoke(ex) else successCallback.invoke(nuid)
}

private[nats] class NatsStreamingSinkWithCompletionStageLogic(
  settings: PublishingSettings,
  promise: Promise[Done],
  shape: SinkShape[OutgoingMessageWithCompletion[Array[Byte]]],
  in: Inlet[OutgoingMessageWithCompletion[Array[Byte]]]
) extends NatsStreamingSinkStageLogic(settings, promise, shape, in){
  def ah(m: OutgoingMessageWithCompletion[Array[Byte]]): AckHandler = (nuid: String, ex: Exception) => if (Option(ex).isDefined) {
    m.promise.tryFailure(ex)
    failureCallback.invoke(ex)
  } else {
    m.promise.trySuccess(Done)
    successCallback.invoke(nuid)
  }
}

private[nats] class  NatsStreamingSimpleSinkStage(settings: PublishingSettings)
  extends GraphStageWithMaterializedValue[SinkShape[OutgoingMessage[Array[Byte]]], Future[Done]]{
  val in: Inlet[OutgoingMessage[Array[Byte]]] = Inlet("NatsStreamingSimpleSink.in")
  val shape: SinkShape[OutgoingMessage[Array[Byte]]] = SinkShape(in)
  def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]
    val logic = new NatsStreamingSimpleSinkStageLogic(settings, promise, shape, in)
    (logic, promise.future)
  }
}

private[nats] class NatsStreamingSinkWithCompletionStage(settings: PublishingSettings)
  extends GraphStageWithMaterializedValue[SinkShape[OutgoingMessageWithCompletion[Array[Byte]]], Future[Done]]{
  val in: Inlet[OutgoingMessageWithCompletion[Array[Byte]]] = Inlet("NatsStreamingSinkWithComplete.in")
  val shape: SinkShape[OutgoingMessageWithCompletion[Array[Byte]]] = SinkShape(in)
  def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]
    val logic = new NatsStreamingSinkWithCompletionStageLogic(settings, promise, shape, in)
    (logic, promise.future)
  }
}
