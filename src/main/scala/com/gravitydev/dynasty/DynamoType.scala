package com.gravitydev.dynasty

import com.amazonaws.services.dynamodbv2.model._
import scala.collection.JavaConverters._
import java.nio.ByteBuffer

/**
 * A type (native or custom) that can be mapped to a DynamoUnderlyingType
 */
trait DynamoType[T] {
  /** Get an actual value from an sdk AttributeValue */
  def get (a: AttributeValue): T

  /** Set an actual value on an sdk AttributeValue */
  def set (a: AttributeValue, value: T): AttributeValue

  /** Get optionally extract a value by name from an sdk map of values */
  def extract (data: Map[String,AttributeValue], name: String): Option[T]

  /** Prepare a value for a PUT operation */
  def put (value: T): Seq[AttributeValue]

  /** Prepare a value for an UPDATE operation */

  /** Prepare a value for a SET operation (i.e. set a value, a set, or delete an empty set) */
  def update (value: T): AttributeValueUpdate
}

sealed trait DynamoNativeType [T] extends DynamoType[T] with DynamoUnderlyingType[T] {
  /** What to return when optionally selecting an empty value */
  def onEmpty: Option[T]

  /** Provide a slightly more informative error message */
  protected def checkNull [X](a: AttributeValue, code: AttributeValue => X): X = {
    val res = code(a)
    if (res == null) sys.error("Null found when reading attribute: " + a)
    res
  }

  def extract (data: Map[String,AttributeValue], name: String): Option[T] = wrapExceptions(s"Error parsing $name from $data") {
    data.get(name) map {get _} orElse onEmpty // recover empty (necessary for set types)
  }
}

object DynamoNativeType {
  def apply[X:DynamoNativeType] = implicitly[DynamoNativeType[X]]
}

/** Marker trait, useful for mappers to handle empty sets */
sealed trait DynamoSetNativeType [T]{self: DynamoNativeType[Set[T]] =>
  def onEmpty = Some(Set.empty[T])
}

/** 
 * A native type as supported by dynamo
 */
trait DynamoUnderlyingType[U]
object DynamoUnderlyingType {
  def apply [T](implicit tpe: DynamoUnderlyingType[T]) = tpe
}

/** Maps a type to another type (custom to (custom|native)) */
class DynamoWrappedType [T,X:DynamoType,U](
  val from: X=>T, 
  val to: T=>X
)(implicit tpe: DynamoType[X] with DynamoUnderlyingType[U]) extends DynamoType[T] with DynamoUnderlyingType[U] {
  def get(a: AttributeValue): T = from(tpe.get(a))
  def extract (data: Map[String,AttributeValue], name: String): Option[T] = tpe.extract(data, name) map from
  def set (a: AttributeValue, value: T): AttributeValue = tpe.set(a, to(value)) 
  def put (value: T): Seq[AttributeValue] = tpe.put(to(value))
  def update (value: T): AttributeValueUpdate = tpe.update(to(value)) 
}

object DynamoType {
  
  @inline def apply [T] (implicit t: DynamoType[T]) = t

  // built-in raw types
  implicit case object NumberT extends DynamoNativeType[BigDecimal] {
    def get (a: AttributeValue): BigDecimal = BigDecimal(checkNull(a, _.getN))
    def set (a: AttributeValue, v: BigDecimal): AttributeValue = {a.setN(v.toString); a}
    def put (value: BigDecimal): Seq[AttributeValue] = Seq(set(new AttributeValue(), value))
    def update (value: BigDecimal): AttributeValueUpdate = new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
    def onEmpty = None
  }
  
  implicit case object NumberSetT extends DynamoNativeType[Set[BigDecimal]] with DynamoSetNativeType[BigDecimal] {
    def get (a: AttributeValue): Set[BigDecimal] = checkNull(a, _.getNS).asScala.toSet.map((x: String) => BigDecimal(x))
    def set (a: AttributeValue, v: Set[BigDecimal]): AttributeValue = {a.setNS(v.map(_.toString).asJava); a}
    def put (value: Set[BigDecimal]): Seq[AttributeValue] = if (value.isEmpty) Nil else Seq(set(new AttributeValue(), value))
    def update (value: Set[BigDecimal]): AttributeValueUpdate = {
      if (value.isEmpty) new AttributeValueUpdate().withAction(AttributeAction.DELETE)
      else new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
    }
  }
  
  implicit case object StringT extends DynamoNativeType[String] {
    def get (a: AttributeValue): String = checkNull(a, _.getS)
    def set (a: AttributeValue, v: String): AttributeValue = {a.setS(v); a}
    def put (value: String): Seq[AttributeValue] = if (value.isEmpty) Nil else Seq(set(new AttributeValue(), value))
    def update (value: String): AttributeValueUpdate = {
      if (value.isEmpty) new AttributeValueUpdate().withAction(AttributeAction.DELETE)
      else new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
    }
    def onEmpty = None
  }
  
  implicit case object StringSetT extends DynamoNativeType[Set[String]] with DynamoSetNativeType[String] {
    def get (a: AttributeValue): Set[String] = checkNull(a, _.getSS).asScala.toSet
    def set (a: AttributeValue, v: Set[String]): AttributeValue = {a.setSS(v.asJava); a}
    def put (value: Set[String]): Seq[AttributeValue] = if (value.isEmpty) Nil else Seq(set(new AttributeValue(), value))
    def update (value: Set[String]): AttributeValueUpdate = {
      if (value.isEmpty) new AttributeValueUpdate().withAction(AttributeAction.DELETE)
      else new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
    }
  }
  
  implicit case object BinaryT extends DynamoNativeType[ByteBuffer] {
    def get (a: AttributeValue): ByteBuffer = checkNull(a, _.getB)
    def set (a: AttributeValue, v: ByteBuffer): AttributeValue = {a.setB(v); a}
    def put (value: ByteBuffer): Seq[AttributeValue] = Seq(set(new AttributeValue(), value))
    def update (value: ByteBuffer): AttributeValueUpdate = new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
    def onEmpty = None
  }
  
  implicit case object BinarySetT extends DynamoNativeType[Set[ByteBuffer]] with DynamoSetNativeType[ByteBuffer] {
    def get (a: AttributeValue): Set[ByteBuffer] = checkNull(a, _.getBS).asScala.toSet
    def set (a: AttributeValue, v: Set[ByteBuffer]): AttributeValue = {a.setBS(v.asJava); a}
    def put (value: Set[ByteBuffer]): Seq[AttributeValue] = if (value.isEmpty) Nil else Seq(set(new AttributeValue(), value))
    def update (value: Set[ByteBuffer]): AttributeValueUpdate = {
      if (value.isEmpty) new AttributeValueUpdate().withAction(AttributeAction.DELETE)
      else new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
    }
  }
  
  implicit case object BooleanT extends DynamoNativeType[Boolean] with DynamoUnderlyingType[Boolean] {
    // force check null to be called on java.lang.Boolean
    // Predef.Boolea2boolean causes NPE on null
    def get (a: AttributeValue): Boolean = {
      if (a.getBOOL != null) checkNull[java.lang.Boolean](a, _.getBOOL)
      else checkNull(a, _.getN == "1") // legacy support for bool as int
    }
  
    def set (a: AttributeValue, v: Boolean): AttributeValue = {a.setBOOL(v); a}
    def put (value: Boolean): Seq[AttributeValue] = Seq(set(new AttributeValue(), value))
    def update (value: Boolean): AttributeValueUpdate = new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
    def onEmpty = None
  }
  
  case object ListT extends DynamoNativeType[List[AttributeValue]] {
    def get (a: AttributeValue): List[AttributeValue] = checkNull(a, _.getL).asScala.toList
    def set (a: AttributeValue, v: List[AttributeValue]): AttributeValue = {
      a.setL(v.asJava)
      a
    }
    def put (value: List[AttributeValue]): Seq[AttributeValue] = Seq(set(new AttributeValue(), value))
    def update (value: List[AttributeValue]): AttributeValueUpdate = new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
    def onEmpty = Some(Nil)
  }
  
  case object MapT extends DynamoNativeType[Map[String,AttributeValue]] {
    def get (a: AttributeValue): Map[String,AttributeValue] = checkNull(a, _.getM).asScala.toMap
    def set (a: AttributeValue, v: Map[String,AttributeValue]): AttributeValue = {
      a.setM(v.asJava)
      a
    }
    def put (value: Map[String,AttributeValue]): Seq[AttributeValue] = Seq(set(new AttributeValue(), value))
    def update (value: Map[String,AttributeValue]): AttributeValueUpdate = new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
    def onEmpty = Some(Map())
  }
  
}
