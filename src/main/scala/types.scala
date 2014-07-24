package com.gravitydev.dynasty

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import java.nio.ByteBuffer
import scala.collection.JavaConverters._

sealed abstract class DynamoType [T](
  val get: AttributeValue => T,
  val getSet: AttributeValue => Set[T],
  _set: (AttributeValue, T) => Unit,
  _setSet: (AttributeValue, Set[T]) => Unit
) {
  val set = {(v: AttributeValue, t: T) => _set(v,t); v}
  val setSet = {(v: AttributeValue, t: Set[T]) => _setSet(v,t); v}
}
object DynamoType {
  // convenience
  def apply[X:DynamoType] = implicitly[DynamoType[X]]
}

// built-in raw types
case object NumberT extends DynamoType [String] (_.getN, _.getNS.asScala.toSet, _ setN _, _ setNS _.asJava)
case object StringT extends DynamoType [String] (_.getS, _.getSS.asScala.toSet, _ setS _, _ setSS _.asJava)
case object BinaryT extends DynamoType [ByteBuffer] (_.getB, _.getBS.asScala.toSet, _ setB _, _ setBS _.asJava)

/**
 * Maps a type to the dynamo API
 * TODO: rename to something else, Mapper doesn't seem too fitting
 */
sealed abstract class DynamoMapper [T] {
  def get: (Map[String,AttributeValue], String) => Option[T]
  def put: T => Seq[AttributeValue]
  def set: T => AttributeValueUpdate
}
object DynamoMapper {
  def apply[T:DynamoMapper] = implicitly[DynamoMapper[T]]
}

@deprecated("You should use customType", "0.0.18-SNAPSHOT")
class DynamoValueMapper [T,X] (val tpe: DynamoType[X])(
  val from: X => T, 
  val to: T => X
) extends DynamoMapper[T] {
  def get = (data, name) => data.get(name) map {v => from(tpe.get(v))}
  def put = value => Seq(tpe.set(new AttributeValue(), to(value)))
  def set = value => new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
}

private [dynasty] class DynamoValueMapper2 [T,X:DynamoType](val from: X=>T, val to: T=>X) extends DynamoMapper[T] {
  def tpe = DynamoType[X]
  def get = (data, name) => data.get(name) map {v => from(DynamoType[X].get(v))}
  def put = value => Seq(DynamoType[X].set(new AttributeValue(), to(value)))
  def set = value => new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
}

class DynamoSetMapper [T,X] (
  val from: X => T, 
  val to: T => X
)(implicit m: DynamoValueMapper2[T,X]) extends DynamoMapper[Set[T]] {
  // empty set can always be retrieved
  def get = (data, name) => Some(data.get(name) map {x => m.tpe.getSet(x) map from} getOrElse Set[T]())
  
  def put = {value =>
    if (value.isEmpty) Nil
    else Seq(m.tpe.setSet(new AttributeValue(), value map {x => m.to(x)}))
  }
  
  def set = {value =>
    if (value.isEmpty) new AttributeValueUpdate().withAction(AttributeAction.DELETE)
    else new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
  }
}

class DynamoCustomMapper [T,X:DynamoMapper](from: X=>T, to: T=>X) extends DynamoMapper[T] {
  def get = (data, name) => DynamoMapper[X].get(data, name) map from
  def put = value => DynamoMapper[X].put(to(value))
  def set = value => DynamoMapper[X].set(to(value)) 
}

