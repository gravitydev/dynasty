package com.gravitydev.dynasty

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import java.nio.ByteBuffer
import scala.collection.JavaConverters._

/** A type (primitive or custom) that can be used with dynamo */
trait DynamoType [T] {
  def get (data: Map[String,AttributeValue], name: String): Option[T]
  def put (value: T): Seq[AttributeValue]
  def set (value: T): AttributeValueUpdate
}
object DynamoType {
  def apply[X:DynamoType] = implicitly[DynamoType[X]]
}

sealed trait DynamoPrimitive[T] {
  def get (a: AttributeValue): T
  def set (a: AttributeValue, v: T): AttributeValue
  def isEmpty (v: T): Boolean = v match {
    case v: Set[_] => v.isEmpty
    case v: String => v.isEmpty
    case _ => false
  }

  def onEmpty: Option[T]
}
object DynamoPrimitive {
  def apply[X:DynamoPrimitive] = implicitly[DynamoPrimitive[X]]
}

/** Marker trait, useful for mappers to handle empty sets */
sealed trait DynamoSetPrimitive [T]{self: DynamoPrimitive[Set[T]] =>
  def onEmpty = Some(Set.empty[T])
}

// built-in raw types
case object NumberT extends DynamoPrimitive[String] {
  def get (a: AttributeValue): String = a.getN
  def set (a: AttributeValue, v: String): AttributeValue = {a.setN(v); a}
  def onEmpty = None
}
case object NumberSetT extends DynamoPrimitive[Set[String]] with DynamoSetPrimitive[String] {
  def get (a: AttributeValue): Set[String] = a.getNS.asScala.toSet
  def set (a: AttributeValue, v: Set[String]): AttributeValue = {a.setNS(v.asJava); a}
}
case object StringT extends DynamoPrimitive[String] {
  def get (a: AttributeValue): String = a.getS
  def set (a: AttributeValue, v: String): AttributeValue = {a.setS(v); a}
  def onEmpty = None
}
case object StringSetT extends DynamoPrimitive[Set[String]] with DynamoSetPrimitive[String] {
  def get (a: AttributeValue): Set[String] = a.getSS.asScala.toSet
  def set (a: AttributeValue, v: Set[String]): AttributeValue = {a.setSS(v.asJava); a}
}

case object BinaryT extends DynamoPrimitive[ByteBuffer] {
  def get (a: AttributeValue): ByteBuffer = a.getB
  def set (a: AttributeValue, v: ByteBuffer): AttributeValue = {a.setB(v); a}
  def onEmpty = None
}
case object BinarySetT extends DynamoPrimitive[Set[ByteBuffer]] with DynamoSetPrimitive[ByteBuffer] {
  def get (a: AttributeValue): Set[ByteBuffer] = a.getBS.asScala.toSet
  def set (a: AttributeValue, v: Set[ByteBuffer]): AttributeValue = {a.setBS(v.asJava); a}
}

case object BooleanT extends DynamoPrimitive[Boolean] {
  def get (a: AttributeValue): Boolean = a.getBOOL
  def set (a: AttributeValue, v: Boolean): AttributeValue = {a.setBOOL(v); a}
  def onEmpty = None
}

case object ListT extends DynamoPrimitive[List[AttributeValue]] {
  def get (a: AttributeValue): List[AttributeValue] = a.getL.asScala.toList
  def set (a: AttributeValue, v: List[AttributeValue]): AttributeValue = {
    a.setL(v.asJava)
    a
  }
  def onEmpty = Some(Nil)
}

case object MapT extends DynamoPrimitive[Map[String,AttributeValue]] {
  def get (a: AttributeValue): Map[String,AttributeValue] = a.getM.asScala.toMap
  def set (a: AttributeValue, v: Map[String,AttributeValue]): AttributeValue = {
    a.setM(v.asJava)
    a
  }
  def onEmpty = Some(Map())
}

/*
class ListT[T,X](implicit ev: DynamoPrimitive[T]) extends DynamoPrimitive [List[T]] {
  def get (a: AttributeValue): List[T] = a.getL.asScala.toList.map(ev.get)
  def set (a: AttributeValue, v: List[T]): AttributeValue = {
    a.setL(v.map(x => ev.set(a,x)).asJava)
    a
  }
  def onEmpty = Some(Nil)
}
class MapT[T,X](implicit ev: DynamoPrimitive[T])
*/


/**
 * Maps a type to the dynamo API
 */
sealed trait DynamoMapper [T] extends DynamoType[T] {
  def get (data: Map[String,AttributeValue], name: String): Option[T]
  def put (value: T): Seq[AttributeValue]
  def set (value: T): AttributeValueUpdate
}
object DynamoMapper {
  def apply[T:DynamoMapper] = implicitly[DynamoMapper[T]] 
}

/** Maps a type to a primitive */
class DynamoPrimitiveMapper [T,X:DynamoPrimitive](val from: X=>T, val to: T=>X) extends DynamoMapper[T] {
  val t = DynamoPrimitive[X]
  def get (data: Map[String,AttributeValue], name: String): Option[T] = {
    data.get(name) map {v => 
      from(t.get(v))
    } orElse t.onEmpty.map(from) // recover empty (necessary for set types)
  }
  def put (value: T): Seq[AttributeValue] = {
    if (t.isEmpty(to(value))) Nil
    else Seq(DynamoPrimitive[X].set(new AttributeValue(), to(value)))
  }
  def set (value: T): AttributeValueUpdate = {
    if (t.isEmpty(to(value))) new AttributeValueUpdate().withAction(AttributeAction.DELETE)
    else new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
  }
}

/** Maps a type to another type (custom to custom) */
class DynamoCustomMapper [T,X:DynamoMapper](val from: X=>T, val to: T=>X) extends DynamoMapper[T] {
  def get (data: Map[String,AttributeValue], name: String): Option[T] = DynamoType[X].get(data, name) map from
  def put (value: T): Seq[AttributeValue] = DynamoType[X].put(to(value))
  def set (value: T): AttributeValueUpdate = DynamoType[X].set(to(value)) 
}

