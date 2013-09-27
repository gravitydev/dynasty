package com.gravitydev.dynasty

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.AmazonWebServiceRequest
import scala.collection.JavaConversions._
import java.nio.ByteBuffer
import scala.concurrent.Future
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.ActorSystem
import scala.language.implicitConversions
import scala.concurrent.{future, promise, Future, Promise}

class Attribute [T](val name: String)(implicit val mapper: DynamoMapper[T]) extends AttributeParser [T] with AttributeSeq[T] {
  def list = List(this)
  override def attributes = List(this)
  
  def parse (m: Map[String,AttributeValue]): Option[T] = mapper.get(m, name)
  
  def ~ [X] (x: Attribute[X]) = new Attribute2(this, x)

  def >> [X](fn: T=>X) = new MappedAttributeSeq(this, fn)
  
  def += [X](value: X)(implicit ev: Set[X] =:= T) = name -> Seq(new AttributeValueUpdate()
    .withAction(AttributeAction.ADD)
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
  def := (value: T) = new AssignmentProxy(name, mapper.put(value), mapper.set(value))
  
  override def toString = "Attribute(name="+name+", mapper="+mapper+")"
  
  def ? = new OptionalAttribute(this)
  
  def isAbsent = Seq(name -> new ExpectedAttributeValue().withExists(false))
  
  def === (v: T) = mapper.put(v) map (x => name -> new ExpectedAttributeValue().withValue(x))
}

class OptionalAttribute [T] (attr: Attribute[T]) extends AttributeParser[Option[T]] with AttributeSeq[Option[T]] {
  def list = attr.list
  def parse (m: Map[String,AttributeValue]) = Some(attr.parse(m))
  override def toString = "OptionalAttribute(" + attr + ")"
}

