package com.gravitydev.dynasty

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.AmazonWebServiceRequest
import scala.collection.JavaConversions._
import java.nio.ByteBuffer
import org.slf4j.LoggerFactory
import scala.language.implicitConversions
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{promise, Future, Promise}
import scala.collection.JavaConversions._

sealed abstract class DynamoType [T](
  val get: AttributeValue => T,
  val getSet: AttributeValue => Set[T],
  _set: (AttributeValue, T) => Unit,
  _setSet: (AttributeValue, Set[T]) => Unit
) {
  val set = {(v: AttributeValue, t: T) => _set(v,t); v}
  val setSet = {(v: AttributeValue, t: Set[T]) => _setSet(v,t); v}
}

sealed abstract class DynamoKeyType[T]
class HashKeyType[H] extends DynamoKeyType[H]
class HashAndRangeKeyType[H,R] extends DynamoKeyType[(H,R)]

sealed trait DynamoKey[T] {
  def === (v: T): Map[String,AttributeValue]
  def in (v: Set[T]): Seq[Map[String,AttributeValue]]
}

object DynamoKey {
}

class HashKey[H] (h: Attribute[H]) extends DynamoKey[H] {
  def === (v: H) = Map(h.name -> h.mapper.put(v).head)
  def in (v: Set[H]) = v.map(x => Map(h.name -> h.mapper.put(x).head)).toSeq
}
class HashAndRangeKey[H,R] (h: Attribute[H], r: Attribute[R]) extends DynamoKey[(H,R)] {
  def === (v: (H,R)) = Map(h.name -> h.mapper.put(v._1).head, r.name -> r.mapper.put(v._2).head)
  def in (v: Set[(H,R)]) = v.map(x => Map(h.name -> h.mapper.put(x._1).head, r.name -> r.mapper.put(x._2).head)).toSeq
}


sealed abstract class DynamoKeyValue (val values: Map[String,AttributeValue])
final class HashKeyValue [H] (hashKey: String, value: AttributeValue) extends DynamoKeyValue(Map(hashKey-> value)) {
  override def toString = "HashKey(" + values + ")"
}
/*
final class HashAndRangeKeyValue [H,R] (values: Map[String,AttributeValue]) extends DynamoKeyValue(values) {
  override def toString = "HashAndRangeKey(" + values + ")"
}
*/

abstract class DynamoTable [K:DynamoKeyType](val tableName: String) {
  def key: DynamoKey[K]
}

// built-in raw types
object NumberT extends DynamoType [String] (_.getN, _.getNS.toSet, _ setN _, _ setNS _)
object StringT extends DynamoType [String] (_.getS, _.getSS.toSet, _ setS _, _ setSS _)
object BinaryT extends DynamoType [ByteBuffer] (_.getB, _.getBS.toSet, _ setB _, _ setBS _)

// mappers
sealed abstract class DynamoMapper [T] {
  def get: (Map[String,AttributeValue], String) => Option[T]
  def put: T => Seq[AttributeValue]
  def set: T => AttributeValueUpdate
}

sealed abstract class DynamoPrimitiveMapper [T] extends DynamoMapper[T]

class DynamoValueMapper [T,X] (val tpe: DynamoType[X])(
  val from: X => T, 
  val to: T => X
) extends DynamoPrimitiveMapper[T] {
  def get = (data, name) => data.get(name) map {v => from(tpe.get(v))}
  def put = value => Seq(tpe.set(new AttributeValue(), to(value)))
  def set = value => new AttributeValueUpdate().withValue(put(value).head).withAction(AttributeAction.PUT)
}

class DynamoSetMapper [T,X] (
  val from: X => T, 
  val to: T => X
)(implicit m: DynamoValueMapper[T,X]) extends DynamoMapper[Set[T]] {
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

object `package` {
 
  // built-in mappers
  implicit def intDynamoType  = new DynamoValueMapper[Int, String]    (NumberT)(_.toInt, _.toString)
  implicit def longDynamoType = new DynamoValueMapper[Long, String]   (NumberT)(_.toLong, _.toString)
  implicit def strDynamoType  = new DynamoValueMapper[String, String] (StringT)(x => x, x => x)
  implicit def boolDynamoType = new DynamoValueMapper[Boolean, String] (NumberT)(_ == 1.toString, if (_) 1.toString else 0.toString)
  implicit def binaryDynamoType = new DynamoValueMapper[ByteBuffer, ByteBuffer] (BinaryT)(x => x, x => x)

  implicit def setDynamoType [T,X](implicit m: DynamoValueMapper[T,X]) = new DynamoSetMapper[T,X](m.from, m.to)

  private[dynasty] type M = Map[String,AttributeValue]
  
  private val logger = LoggerFactory getLogger getClass

  // key
  implicit def attrToKey[H](h: Attribute[H]): DynamoKey[H] = new HashKey(h)
  implicit def attrToKey2[H,R](hr: (Attribute[H], Attribute[R])): DynamoKey[(H,R)] = new HashAndRangeKey(hr._1, hr._2)

  // utility
  private [dynasty] def withAsyncHandler [R<:AmazonWebServiceRequest,T] (fn: AsyncHandler[R,T] => Unit): Future[T] = {
    val p = promise[T]
    fn {
      new AsyncHandler [R,T] {
        def onError (ex: Exception) = p failure ex
        def onSuccess (r: R, x: T) = p success x
      }
    }
    p.future
  }

  def handler [R<:AmazonWebServiceRequest,X](p: Promise[X]) = new AsyncHandler [R,X] {
    def onError (ex: Exception) = p failure ex
    def onSuccess (r: R, x: X) = p success x
  }

  
  private [dynasty] def logging [T](tag: String)(f: Future[T]) = f //recover {case e => logger.error(tag + ": Request error", e); throw e}
  
  implicit def tableToQueryBuilder [K,T<:DynamoTable[K]] (table: T with DynamoTable[K]) = new QueryBuilder(table)
 
  implicit def fromString (value: String) = new AttributeValue().withS(value)
  implicit def fromInt (value: Int) = new AttributeValue().withN(value.toString)
  implicit def fromLong (value: Long) = new AttributeValue().withN(value.toString)

  implicit def assignmentToPut (value: AssignmentProxy) = value.name -> value.put
  implicit def assignmentToSet (value: AssignmentProxy) = value.name -> value.set
 
  /*
  def itemRequest [K<:DynamoKey, T<:DynamoTable[K], V<:Any](table: T, key: K*)(attributes: T => AttributeSeq[V]) = {
    (table.tableName, key.map(_.key).toSeq, attributes(table).attributes map {_.name})
  }
  */
 
  /*
  def key [H](hash: H)(implicit hev: H => (String,AttributeValue)) = new HashKey[H](Map(hev(hash)))
  def key [H,R](hash: H, range: R)(implicit hev: H => (String,AttributeValue), rev: R => (String,AttributeValue)) = new HashAndRangeKey[H,R](Map(hev(hash), rev(range)))
  */
  
  private [dynasty] type Z[X] = AttributeParser[X]

  def attr [T] (name: String)(implicit att: DynamoMapper[T]) = new Attribute [T](name) 

  implicit def toHashKeyType[H:DynamoMapper]: DynamoKeyType[H] = new HashKeyType[H]
  implicit def toHashAndRangeKeyType[H:DynamoMapper, R:DynamoMapper]: DynamoKeyType[(H,R)] = new HashAndRangeKeyType[H,R]

}

trait AttributeParser [T] {
  def list: List[AttributeParser[_]]
  def attributes: List[Attribute[_]]
  def parse (m: M): Option[T]
  def map [X](fn: T=>X) = new MappedAttributeSeq(this, fn)
  def ? = new OptionalAttributeParser(this)
}

class OptionalAttributeParser[T](parser: AttributeParser[T]) extends AttributeParser[Option[T]] {
  def parse (m: M) = Some(parser.parse(m))
  def attributes = parser.attributes
  def list = parser.list
}

trait AttributeSeq [T] extends AttributeParser [T] {
  def attributes = list.foldLeft(List[Attribute[_]]())(_ ++ _.attributes)
  def parse (m: M): Option[T]
  override def toString = list.toString
}

class MappedAttributeSeq [A,B](attr: AttributeParser[A], fn: A=>B) extends AttributeSeq [B] {
  def parse (m: M) = attr.parse(m) map fn
  def list = attr.list
  def ~ [X] (x: Attribute[X]) = new Attribute2(this, x)
} 

class Attribute2 [A,B](a:Z[A], b:Z[B]) extends AttributeSeq[(A,B)] {
  def list = List(a,b)
  def parse (m: M) = for (av <- a parse m; bv <- b parse m) yield (av,bv)
  def ~ [C] (c: Z[C]) = new Attribute3(a,b,c)
  def >> [V] (fn: (A,B)=>V) = map(fn.tupled)
}
class Attribute3 [A,B,C](a:Z[A], b:Z[B], c:Z[C]) extends AttributeSeq[(A,B,C)] {
  def list = List(a,b,c)
  def parse (m: M) = for (av <- a parse m; bv <- b parse m; cv <- c parse m) yield (av,bv,cv)
  def ~ [D] (d: Z[D]) = new Attribute4(a,b,c,d)
  def >> [V] (fn: (A,B,C)=>V) = map(fn.tupled)
}
class Attribute4 [A,B,C,D](a: Z[A], b:Z[B], c:Z[C], d:Z[D]) extends AttributeSeq[(A,B,C,D)] {
  def list = List(a,b,c,d)
  def parse (m: M) = for (av <- a parse m; bv <- b parse m; cv <- c parse m; dv <- d parse m) yield (av,bv,cv,dv)
  def ~ [E] (e: Z[E]) = new Attribute5(a,b,c,d,e)
  def >> [V] (fn: (A,B,C,D)=>V) = map(fn.tupled)
}
class Attribute5 [A,B,C,D,E](a:Z[A], b:Z[B], c:Z[C], d:Z[D], e:Z[E]) extends AttributeSeq[(A,B,C,D,E)] {
  def list = List(a,b,c,d,e)
  def parse (m: M) = for (av <- a parse m; bv<- b parse m; cv <- c parse m; dv <- d parse m; ev <- e parse m) yield (av,bv,cv,dv,ev)
  def ~ [F] (f: Z[F]) = new Attribute6(a,b,c,d,e,f)
  def >> [V] (fn: (A,B,C,D,E)=>V) = map(fn.tupled)
}
class Attribute6 [A,B,C,D,E,F](a:Z[A], b:Z[B], c:Z[C], d:Z[D], e:Z[E], f:Z[F]) extends AttributeSeq[(A,B,C,D,E,F)] {
  def list = List(a,b,c,d,e,f)
  def parse (m: M) = for (av <- a parse m; bv <- b parse m; cv <- c parse m; dv <- d parse m; ev <- e parse m; fv <- f parse m) yield (av,bv,cv,dv,ev,fv)
  def >> [V] (fn: (A,B,C,D,E,F)=>V) = map(fn.tupled)
}


