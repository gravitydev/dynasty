package com.gravitydev.dynasty

import scala.util.control.Exception.allCatch
import scala.util.Try
import scala.concurrent.ExecutionContext
import akka.actor.Scheduler
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import java.util.concurrent.{Future => JavaFuture}
import java.util.concurrent.{Future => JavaFuture}
import scala.language.implicitConversions
import org.slf4j.Logger

object concurrent {
  implicit def toAkkaFuture [T](javaFuture: JavaFuture[T])(implicit ec: ExecutionContext, scheduler: Scheduler) = wrapJavaFutureInAkkaFuture(javaFuture)
  
  private def wrapJavaFutureInAkkaFuture[T](javaFuture: JavaFuture[T], maybeTimeout: Option[FiniteDuration] = None)(implicit ec: ExecutionContext, scheduler: Scheduler):
    scala.concurrent.Future[T] = {
    val promise = scala.concurrent.promise[T]
    pollJavaFutureUntilDoneOrCancelled(javaFuture, promise, maybeTimeout.map(t => Deadline.now + t))
    promise.future
  }
  
  private def pollJavaFutureUntilDoneOrCancelled[T](
    javaFuture: JavaFuture[T], 
    promise: scala.concurrent.Promise[T], 
    maybeTimeout: Option[Deadline] = None
  )(implicit scheduler: Scheduler, ec: ExecutionContext) {
    //import system.dispatcher

    if (maybeTimeout.exists(_.isOverdue)) javaFuture.cancel(true);
  
    if (javaFuture.isDone || javaFuture.isCancelled) {
      promise.complete(Try { javaFuture.get })
    } else {
      scheduler.scheduleOnce(10 milliseconds) {
        pollJavaFutureUntilDoneOrCancelled(javaFuture, promise, maybeTimeout)
      }
    }
  }
}

