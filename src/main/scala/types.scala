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

  /** Provide a slightly more informative error message */
  protected def checkNull [X](a: AttributeValue, code: AttributeValue => X): X = {
    val res = code(a)
    if (res == null) sys.error("Null found when reading attribute: " + a)
    res
  }
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
  def get (a: AttributeValue): String = checkNull(a, _.getN)
  def set (a: AttributeValue, v: String): AttributeValue = {a.setN(v); a}
  def onEmpty = None
}
case object NumberSetT extends DynamoPrimitive[Set[String]] with DynamoSetPrimitive[String] {
  def get (a: AttributeValue): Set[String] = checkNull(a, _.getNS).asScala.toSet
  def set (a: AttributeValue, v: Set[String]): AttributeValue = {a.setNS(v.asJava); a}
}
case object StringT extends DynamoPrimitive[String] {
  def get (a: AttributeValue): String = checkNull(a, _.getS)
  def set (a: AttributeValue, v: String): AttributeValue = {a.setS(v); a}
  def onEmpty = None
}
case object StringSetT extends DynamoPrimitive[Set[String]] with DynamoSetPrimitive[String] {
  def get (a: AttributeValue): Set[String] = checkNull(a, _.getSS).asScala.toSet
  def set (a: AttributeValue, v: Set[String]): AttributeValue = {a.setSS(v.asJava); a}
}

case object BinaryT extends DynamoPrimitive[ByteBuffer] {
  def get (a: AttributeValue): ByteBuffer = checkNull(a, _.getB)
  def set (a: AttributeValue, v: ByteBuffer): AttributeValue = {a.setB(v); a}
  def onEmpty = None
}
case object BinarySetT extends DynamoPrimitive[Set[ByteBuffer]] with DynamoSetPrimitive[ByteBuffer] {
  def get (a: AttributeValue): Set[ByteBuffer] = checkNull(a, _.getBS).asScala.toSet
  def set (a: AttributeValue, v: Set[ByteBuffer]): AttributeValue = {a.setBS(v.asJava); a}
}

case object BooleanT extends DynamoPrimitive[Boolean] {
  // force check null to be called on java.lang.Boolean
  // Predef.Boolea2boolean causes NPE on null
  def get (a: AttributeValue): Boolean = checkNull[java.lang.Boolean](a, _.getBOOL)

  def set (a: AttributeValue, v: Boolean): AttributeValue = {a.setBOOL(v); a}
  def onEmpty = None
}

case object ListT extends DynamoPrimitive[List[AttributeValue]] {
  def get (a: AttributeValue): List[AttributeValue] = checkNull(a, _.getL).asScala.toList
  def set (a: AttributeValue, v: List[AttributeValue]): AttributeValue = {
    a.setL(v.asJava)
    a
  }
  def onEmpty = Some(Nil)
}

case object MapT extends DynamoPrimitive[Map[String,AttributeValue]] {
  def get (a: AttributeValue): Map[String,AttributeValue] = checkNull(a, _.getM).asScala.toMap
  def set (a: AttributeValue, v: Map[String,AttributeValue]): AttributeValue = {
    a.setM(v.asJava)
    a
  }
  def onEmpty = Some(Map())
}

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

