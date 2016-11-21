package com.gravitydev.dynasty

import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.AmazonWebServiceRequest
import scala.concurrent.{Future, Promise}

private [dynasty] object Util {

  private class AwsAsyncPromiseHandler [R<:AmazonWebServiceRequest,T] (promise: Promise[T]) extends AsyncHandler [R,T] {
    def onError (ex: Exception) = promise failure ex
    def onSuccess (r: R, x: T) = promise success x
  }

  def awsToScala [R<:AmazonWebServiceRequest,T](fn: Function2[R,AsyncHandler[R,T],java.util.concurrent.Future[T]]): Function1[R,Future[T]] = {req =>
    val p = Promise[T]
    fn(req, new AwsAsyncPromiseHandler(p) )
    p.future
  }
  
  /** Optionally apply a function */
  implicit class FluentInterfaceWrapper [A](subject: A) {
    def optionallyApply [T](opt: Option[T])(fn: (A,T) => A): A = {
      opt map (t => fn(subject, t)) getOrElse subject
    }
  }
  
}
