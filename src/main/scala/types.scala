package com.gravitydev.dynasty

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import java.nio.ByteBuffer
import scala.collection.JavaConverters._

/** A type (primitive or custom) that can be used with dynamo */
trait DynamoType [T] {
  def get: (Map[String,AttributeValue], String) => Option[T]
  def put: T => Seq[AttributeValue]
  def set: T => AttributeValueUpdate
}
object DynamoType {
  def apply[X:DynamoType] = implicitly[DynamoType[X]]
}

sealed trait DynamoPrimitive[T] {
  def get (a: AttributeValue): T
  def set (a: AttributeValue, v: T): AttributeValue
}
object DynamoPrimitive {
  def apply[X:DynamoPrimitive] = implicitly[DynamoPrimitive[X]]
}

// built-in raw types
case object NumberT extends DynamoPrimitive[String] {
  def get (a: AttributeValue): String = a.getN
  def set (a: AttributeValue, v: String): AttributeValue = {a.setN(v); a}
}
case object NumberSetT extends DynamoPrimitive[Set[String]] {
  def get (a: AttributeValue): Set[String] = a.getNS.asScala.toSet
  def set (a: AttributeValue, v: Set[String]): AttributeValue = {a.setNS(v.asJava); a}
}
case object StringT extends DynamoPrimitive[String] {
  def get (a: AttributeValue): String = a.getS
  def set (a: AttributeValue, v: String): AttributeValue = {a.setS(v); a}
}
case object StringSetT extends DynamoPrimitive[Set[String]] {
  def get (a: AttributeValue): Set[String] = a.getSS.asScala.toSet
  def set (a: AttributeValue, v: Set[String]): AttributeValue = {a.setSS(v.asJava); a}
}

case object BinaryT extends DynamoPrimitive[ByteBuffer] {
  def get (a: AttributeValue): ByteBuffer = a.getB
  def set (a: AttributeValue, v: ByteBuffer): AttributeValue = {a.setB(v); a}
}
case object BinarySetT extends DynamoPrimitive[Set[ByteBuffer]] {
  def get (a: AttributeValue): Set[ByteBuffer] = a.getBS.asScala.toSet
  def set (a: AttributeValue, v: Set[ByteBuffer]): AttributeValue = {a.setBS(v.asJava); a}
}

case object BooleanT extends DynamoPrimitive[Boolean] {
  def get (a: AttributeValue): Boolean = a.getBOOL
  def set (a: AttributeValue, v: Boolean): AttributeValue = {a.setBOOL(v); a}
}


/**
 * Maps a type to the dynamo API
 */
sealed trait DynamoMapper [T] extends DynamoType[T] {
  def get: (Map[String,AttributeValue], String) => Option[T]
  def put: T => Seq[AttributeValue]
  def set: T => AttributeValueUpdate
}
object DynamoMapper {
  def apply[T:DynamoMapper] = implicitly[DynamoMapper[T]] 
}

/** Maps a type to a primitive */
class DynamoPrimitiveMapper [T,X:DynamoPrimitive](val from: X=>T, val to: T=>X) extends DynamoMapper[T] {
  def get = (data, name) => data.get(name) map {v => from(DynamoPrimitive[X].get(v))}
  def put = value => Seq(DynamoPrimitive[X].set(new AttributeValue(), to(value)))
  def set = value => new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
}

/** Maps a type to another type (custom to custom) */
class DynamoCustomMapper [T,X:DynamoMapper](val from: X=>T, val to: T=>X) extends DynamoMapper[T] {
  def get = (data, name) => DynamoType[X].get(data, name) map from
  def put = value => DynamoType[X].put(to(value))
  def set = value => DynamoType[X].set(to(value)) 
}

/*
class DynamoSetMapper [T](implicit m: DynamoMapperSingle[T]) extends DynamoMapper[Set[T]] {
  // empty set can always be retrieved
  def get = (data, name) => Some(data.get(name) map {x => m.getSet(x)} getOrElse Set[T]())
  
  def put = {value =>
    if (value.isEmpty) Nil
    else {
      val av = new AttributeValue()
      m.setSet(av, value)
      Seq(av)
    }
  }
  
  def set = {value =>
    if (value.isEmpty) new AttributeValueUpdate().withAction(AttributeAction.DELETE)
    else new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
  }
}
*/
