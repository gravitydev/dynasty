package com.gravitydev.dynasty

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.AmazonWebServiceRequest
import scala.collection.JavaConversions._
import java.nio.ByteBuffer
import scala.concurrent.Future
import org.slf4j.LoggerFactory
import scala.language.implicitConversions
import scala.concurrent.{future, promise, Future, Promise}

class Attribute [T:DynamoType](val name: String) extends Attribute1[T] {
  val mapper = implicitly[DynamoType[T]]

  def list = List(this)

  override def attributes = List(this)
  
  def parse (m: Map[String,AttributeValue]): Option[T] = mapper.get(m, name)
  
  def += [X](value: X)(implicit ev: Set[X] =:= T) = name -> Seq(new AttributeValueUpdate()
    .withAction(AttributeAction.ADD)
    .withValue(mapper.put(ev(Set(value))).head)) // hackish...

  def -= [X](value: X)(implicit ev: Set[X] =:= T) = name -> Seq(new AttributeValueUpdate()
    .withAction(AttributeAction.DELETE)
    .withValue(mapper.put(ev(Set(value))).head)) // hackish...
    
  def + (value: T)(implicit ev: Int =:= T) = name -> Seq(new AttributeValueUpdate()
    .withAction(AttributeAction.ADD)
    .withValue(mapper.put(value).head)) // hackish
   
  def --= [X](value: Set[X])(implicit ev: Set[X] =:= T) = name -> {
    if (value.isEmpty) Nil // TODO: this should be a safe operation
    else Seq(new AttributeValueUpdate()
      .withAction(AttributeAction.DELETE)
      .withValue(mapper.put(ev(value)).head))
  }
    
  def ++= [X](value: Set[X])(implicit ev: Set[X] =:= T) = name -> {
    if (value.isEmpty) Nil // TODO: this should be a safe operation
    else Seq(name -> new AttributeValueUpdate()
      .withAction(AttributeAction.ADD)
      .withValue(mapper.put(ev(value)).head))
  }
    
  // produce an assignment, let the implicit conversion create an update or a value
  def := (value: T) = new AssignmentTerm(name, mapper.put(value), Seq(mapper.set(value)))

  def :? (value: Option[T]) = value map {v =>
    new AssignmentTerm(name, mapper.put(v), Seq(mapper.set(v)))
  } getOrElse new AssignmentTerm(name, Nil, Nil)
  
  override def toString = "Attribute(name="+name+", mapper="+mapper+")"
 
  @deprecated("Use isNull", "0.2.1")
  def isAbsent = isNull

  def isNull = new UnaryOp(this, ComparisonOperator.NULL) //Seq(name -> new ExpectedAttributeValue().withExists(false))
 
  // comparisons
  def === (v: T) = new ComparisonEquals(this, v)
  def <   (v: T) = new Comparison(this, ComparisonOperator.LT, v)
  def <=  (v: T) = new Comparison(this, ComparisonOperator.LE, v)
  def >   (v: T) = new Comparison(this, ComparisonOperator.GT, v)
  def >=  (v: T) = new Comparison(this, ComparisonOperator.GE, v)
  def beginsWith (v: T)(implicit ev: String =:= T) = new Comparison(this, ComparisonOperator.BEGINS_WITH, v)

  def between (a: T, b: T) = new BetweenComparison(this, a, b)
}

