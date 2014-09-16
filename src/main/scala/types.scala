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

/** One of the dynamo primitive types: Number, String, and Binary */
sealed abstract class DynamoPrimitive [T](
  val get: AttributeValue => T,
  val getSet: AttributeValue => Set[T],
  _set: (AttributeValue, T) => Unit,
  _setSet: (AttributeValue, Set[T]) => Unit
) {
  val set = {(v: AttributeValue, t: T) => _set(v,t); v}
  val setSet = {(v: AttributeValue, t: Set[T]) => _setSet(v,t); v}
}
object DynamoPrimitive {
  // convenience
  def apply[X:DynamoPrimitive] = implicitly[DynamoPrimitive[X]]
}

// built-in raw types
case object NumberT extends DynamoPrimitive [String] (_.getN, _.getNS.asScala.toSet, _ setN _, _ setNS _.asJava)
case object StringT extends DynamoPrimitive [String] (_.getS, _.getSS.asScala.toSet, _ setS _, _ setSS _.asJava)
case object BinaryT extends DynamoPrimitive [ByteBuffer] (_.getB, _.getBS.asScala.toSet, _ setB _, _ setBS _.asJava)

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

/**
 * Maps a type to a 'single' type (non set)
 */
sealed abstract class DynamoMapperSingle [T] extends DynamoType[T] {
  def getSet (s: AttributeValue): Set[T]
  def setSet (v: AttributeValue, s: Set[T]): Unit
}
object DynamoMapperSingle {
  def apply[T:DynamoMapperSingle] = implicitly[DynamoMapperSingle[T]] 
}

/** Maps a type to a primitive */
class DynamoPrimitiveMapper [T,X:DynamoPrimitive](val from: X=>T, val to: T=>X) extends DynamoMapperSingle[T] {
  def get = (data, name) => data.get(name) map {v => from(DynamoPrimitive[X].get(v))}
  def put = value => Seq(DynamoPrimitive[X].set(new AttributeValue(), to(value)))
  def set = value => new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
  def getSet (s: AttributeValue): Set[T] = DynamoPrimitive[X].getSet(s) map from
  def setSet (v: AttributeValue, s: Set[T]): Unit = DynamoPrimitive[X].setSet(v, s map to)
}

/** Maps a type to another type (custom to custom) */
class DynamoCustomMapper [T,X:DynamoMapperSingle](val from: X=>T, val to: T=>X) extends DynamoMapperSingle[T] {
  def get = (data, name) => DynamoType[X].get(data, name) map from
  def put = value => DynamoType[X].put(to(value))
  def set = value => DynamoType[X].set(to(value)) 
  def getSet (s: AttributeValue): Set[T] = DynamoMapperSingle[X].getSet(s) map from
  def setSet (v: AttributeValue, s: Set[T]): Unit = DynamoMapperSingle[X].setSet(v, s map to)
}

@deprecated("You should use customType", "0.0.18-SNAPSHOT")
class DynamoValueMapper [T,X] (val tpe: DynamoPrimitive[X]) (
  from: X => T,
  to: T => X
) extends DynamoMapper[T] {
  val get: (Map[String,AttributeValue], String) => Option[T] = (data, name) => data.get(name) map {v => from(tpe.get(v))}
  val put: T => Seq[AttributeValue] = value => Seq(tpe.set(new AttributeValue(), to(value)))
  val set: T => AttributeValueUpdate = value => new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
  def getSet (s: AttributeValue): Set[T] = tpe.getSet(s) map from
  def setSet (v: AttributeValue, s: Set[T]): Unit = tpe.setSet(v, s map to)
}

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

