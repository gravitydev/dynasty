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

class AssignmentProxy (val name: String, val put: Seq[AttributeValue], val set: AttributeValueUpdate)

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
  
  def exists (e: Boolean) = Seq(name -> new ExpectedAttributeValue().withExists(e))
  
  def value (v: T) = mapper.put(v) map (x => name -> new ExpectedAttributeValue().withValue(x))

  def === (v: T) = new KeyValue(name, mapper.put(v).head)
}

class OptionalAttribute [T] (attr: Attribute[T]) extends AttributeParser[Option[T]] with AttributeSeq[Option[T]] {
  def list = attr.list
  def parse (m: Map[String,AttributeValue]) = Some(attr.parse(m))
  override def toString = "OptionalAttribute(" + attr + ")"
}

sealed abstract class DynamoType [T](
  val get: AttributeValue => T,
  val getSet: AttributeValue => Set[T],
  _set: (AttributeValue, T) => Unit,
  _setSet: (AttributeValue, Set[T]) => Unit
) {
  val set = {(v: AttributeValue, t: T) => _set(v,t); v}
  val setSet = {(v: AttributeValue, t: Set[T]) => _setSet(v,t); v}
}

sealed abstract class DynamoKey (val key: Map[String,AttributeValue])
final class HashKey [H] (key: Map[String,AttributeValue]) extends DynamoKey(key) {
  override def toString = "HashKey(" + key + ")"
}
final class HashAndRangeKey [H,R] (key: Map[String,AttributeValue]) extends DynamoKey(key) {
  override def toString = "HashAndRangeKey(" + key + ", range=" + key + ")"
}

abstract class DynamoTable [K <: DynamoKey](val tableName: String)

object `package` {
  private type M = Map[String,AttributeValue]
  
  private val logger = LoggerFactory getLogger getClass

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
  
  /*
  class OptionalAttributeParser[T](parser: AttributeParser[T]) extends AttributeParser[Option[T]] {
    def parse (m: M) = Some(parser.parse(m))
  }
  */
 
  implicit def fromString (value: String) = new AttributeValue().withS(value)
  implicit def fromInt (value: Int) = new AttributeValue().withN(value.toString)
  implicit def fromLong (value: Long) = new AttributeValue().withN(value.toString)

  implicit def assignmentToPut (value: AssignmentProxy) = value.name -> value.put
  implicit def assignmentToSet (value: AssignmentProxy) = value.name -> Seq(value.set)
  
  def itemRequest [K<:DynamoKey, T<:DynamoTable[K], V<:Any](table: T, key: K*)(attributes: T => AttributeSeq[V]) = {
    (table.tableName, key.map(_.key).toSeq, attributes(table).attributes map {_.name})
  }
  
  def key [H](hash: H)(implicit hev: H => (String,AttributeValue)) = new HashKey[H](Map(hev(hash)))
  def key [H,R](hash: H, range: R)(implicit hev: H => (String,AttributeValue), rev: R => (String,AttributeValue)) = new HashAndRangeKey[H,R](Map(hev(hash), rev(range)))

  object NumberType extends DynamoType [String] (_.getN, _.getNS.toSet, _ setN _, _ setNS _)
  object StringType extends DynamoType [String] (_.getS, _.getSS.toSet, _ setS _, _ setSS _)
  object BinaryType extends DynamoType [ByteBuffer] (_.getB, _.getBS.toSet, _ setB _, _ setBS _)

  sealed abstract class DynamoMapper [T] {
    def get: (Map[String,AttributeValue], String) => Option[T]
    def put: T => Seq[AttributeValue]
    def set: T => AttributeValueUpdate
  }

  class DynamoValueMapper [T,X] (
    val tpe: DynamoType[X]
  )(
    val from: X => T, 
    val to: T => X
  ) extends DynamoMapper[T] {
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
  
  implicit def intDynamoType  = new DynamoValueMapper[Int, String]    (NumberType)(_.toInt, _.toString)
  implicit def longDynamoType = new DynamoValueMapper[Long, String]   (NumberType)(_.toLong, _.toString)
  implicit def strDynamoType  = new DynamoValueMapper[String, String] (StringType)(x => x, x => x)
  implicit def boolDynamoType = new DynamoValueMapper[Boolean, String] (NumberType)(_ == 1.toString, if (_) 1.toString else 0.toString)
  implicit def binaryDynamoType = new DynamoValueMapper[ByteBuffer, ByteBuffer] (BinaryType)(x => x, x => x)

  implicit def setDynamoType [T,X](implicit m: DynamoValueMapper[T,X]) = new DynamoSetMapper[T,X](m.from, m.to)
  
  trait AttributeParser [T] {
    def list: List[AttributeParser[_]]
    def attributes: List[Attribute[_]]
    def parse (m: M): Option[T]
    def map [X](fn: T=>X) = new MappedAttributeSeq(this, fn)
    //def ? = new OptionalAttributeParser(this)
  }
  
  private type Z[X] = AttributeParser[X]

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
  
  def attr [T] (name: String)(implicit att: DynamoMapper[T]) = new Attribute [T](name) 

}

