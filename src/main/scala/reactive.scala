package com.gravitydev.dynasty

import org.reactivestreams._
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._
import com.gravitydev.awsutil.awsToScala
import com.typesafe.scalalogging.StrictLogging

class DynamoScanPublisher[T] (
  dynamoClient: AmazonDynamoDBAsyncClient,
  scanRequest: ScanRequest
)(implicit ec: ExecutionContext) extends Publisher[T] with StrictLogging {
  def subscribe(s: Subscriber[_ >: T]) = {
    if(s eq null) throw new NullPointerException("Subscriber is null")

    logger.info("SUBSCRIBING")

    val subscription = new DynamoSubscription(
      s,
      lastKeyOpt => {
        awsToScala(dynamoClient.scanAsync)(
          lastKeyOpt.map(lastKey => scanRequest.withExclusiveStartKey(lastKey)).getOrElse(scanRequest)
        )
      }
    )

    logger.info("CREATED SUBSCRIPTION")
    s.onSubscribe(subscription) 
  }
}

class DynamoSubscription (
  subscriber: Subscriber[_],
  queryNextBatchFn: Option[java.util.Map[String,AttributeValue]] => Future[ScanResult]
)(implicit ec: ExecutionContext) extends Subscription with StrictLogging {
  private var lastKey: Option[java.util.Map[String,AttributeValue]] = None

  /** Whether the Subscriber has been signaled with `onComplete` or `onError`. */
  @volatile private[this] var finished = false

  private[this] val remaining = new AtomicLong(0L)

  @volatile private[this] var cancelRequested = false

  @volatile private[this] var querying = false

  private def tryGetData(): Unit = {
    logger.info(s"GETTING DATA ${remaining.get} $querying")
    if (remaining.get > 0 && !querying) {
      querying = true
      queryNextBatchFn(lastKey).onComplete {
        case Success(result) => {
          logger.info("SUCCESS")
          subscriber.asInstanceOf[Subscriber[Any]].onNext(result)
          if (result.getLastEvaluatedKey() == null) {
            subscriber.onComplete()
            finished = true
            logger.info("COMPLETED")
          }
          remaining.getAndAdd(-1)
          lastKey = Option(result.getLastEvaluatedKey())
          querying = false
          tryGetData()
        }
        case Failure(e) => {
          logger.error("ERROR", e)
          subscriber.onError(e)
          querying = false
        }
      }
    }
  }
  
  def request (n: Long): Unit = {
    logger.info(s"REQUESTING $n, remaining: ${remaining.get}")
    if (n <= 0) subscriber.onError(new IllegalArgumentException("Requested <= 0"))
    if (!cancelRequested) {
      remaining.addAndGet(n)
      tryGetData()
    }
  }

  def cancel (): Unit = {
    cancelRequested = false
    remaining.getAndSet(0L)
  }
 
}

