package com.gravitydev.dynasty

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.AmazonWebServiceRequest
import scala.collection.JavaConversions._
import java.nio.ByteBuffer
import com.typesafe.scalalogging.StrictLogging
import scala.language.implicitConversions
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.collection.JavaConversions._


sealed abstract class DynamoKeyType[T]
class HashKeyType[H] extends DynamoKeyType[H]
class HashAndRangeKeyType[H,R] extends DynamoKeyType[(H,R)]

sealed trait DynamoKey[T] {
  def === (v: T): Map[String,AttributeValue]
  def in (v: Set[T]): Seq[Map[String,AttributeValue]]
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

class DynamoIndex[+T<:DynamoTable[_]](val name: String)

abstract class DynamoTable [K:DynamoKeyType](val tableName: String) {
  def key: DynamoKey[K]

  protected def attr [T] (name: String)(implicit att: DynamoType[T]) = new Attribute [T](name) 

  protected def index (name: String): DynamoIndex[this.type] = new DynamoIndex[this.type](name)
}

class DynamoTypeBuilder [T,X] {
  def apply [U](from: X=>T, to: T=>X)(implicit tpe: DynamoType[X] with DynamoUnderlyingType[U]) = new DynamoWrappedType[T,X,U](from, to)
}

object `package` extends StrictLogging {

  private [dynasty] def wrapExceptions [A](msg: => String)(fn: => A): A = try fn catch {
    case e: Exception => throw new Exception(msg, e)
  }

  def customType [T,X] = new DynamoTypeBuilder[T,X]

  def literal[T](x: T) = new LiteralAttributeParser[T](x)
 
  // built-in types
  implicit val decimalType        = NumberT 
  implicit val decimalSetType     = NumberSetT 

  implicit val strDynamoType      = StringT 
  implicit val strSetDynamoType   = StringSetT 

  implicit val binaryDynamoType   = BinaryT
  implicit val boolDynamoType     = BooleanT 
  implicit def listDynamoType     = ListT
  implicit def mapDynamoType      = MapT

  implicit def intDynamoType      = customType[Int, BigDecimal]              (_.toInt, BigDecimal(_))(NumberT)
  implicit def intSetDynamoType   = customType[Set[Int], Set[BigDecimal]]    (_.map(_.toInt), _.map(BigDecimal(_)))(NumberSetT)

  implicit def longDynamoType     = customType[Long, BigDecimal]             (_.toLong, BigDecimal(_))(NumberT)
  implicit def longSetDynamoType  = customType[Set[Long], Set[BigDecimal]]   (_.map(_.toLong), _.map(BigDecimal(_)))(NumberSetT)


  private[dynasty] type M = Map[String,AttributeValue]

  // key
  implicit def attrToKey[H](h: Attribute[H]): DynamoKey[H] = new HashKey(h)
  implicit def attrToKey2[H,R](hr: (Attribute[H], Attribute[R])): DynamoKey[(H,R)] = new HashAndRangeKey(hr._1, hr._2)

  private [dynasty] def logging [T](tag: String)(f: Future[T]) = f //recover {case e => logger.error(tag + ": Request error", e); throw e}
  
  implicit def tableToQueryBuilder [K,T<:DynamoTable[K]] (table: T with DynamoTable[K]) = new QueryBuilder(table)
 
  implicit def assignmentToPut (value: AssignmentTerm) = value.name -> value.put
  implicit def assignmentToSet (value: AssignmentTerm) = value.name -> value.set

  // TODO: Are expectations deprecated? 
  implicit def equalsToExpectation [T](comp: ComparisonEquals[T]) = 
    comp.values map (x => comp.attr.name -> new ExpectedAttributeValue().withValue(x))

  implicit def unaryOpToExpectation [T](op: UnaryOp[T]): Seq[(String,ExpectedAttributeValue)] = op.op match {
    case ComparisonOperator.NULL => Seq(op.attr.name -> new ExpectedAttributeValue().withExists(false))
    case ComparisonOperator.NOT_NULL => Seq(op.attr.name -> new ExpectedAttributeValue().withExists(false))
  }

  implicit def comparisonToCondition [T](comp: ComparisonTerm[T]): SingleConditionExpr = 
    SingleConditionExpr(
      comp.attr.name, 
      new Condition()
        .withComparisonOperator(comp.op)
        .withAttributeValueList(comp.values)
    )

  
  private [dynasty] type Z[X] = AttributeParser[X]

  implicit def toHashKeyType[H:DynamoType]: DynamoKeyType[H] = new HashKeyType[H]
  implicit def toHashAndRangeKeyType[H:DynamoType, R:DynamoType]: DynamoKeyType[(H,R)] = new HashAndRangeKeyType[H,R]

}

trait AttributeParser [T] {
  def list: List[AttributeParser[_]]
  def attributes: List[Attribute[_]]
  def parse (m: M): Option[T]
  def map [X](fn: T=>X) = new MappedAttributeSeq(this, fn)
  def ? = new OptionalAttributeParser(this)
}

trait AttributeSeq [T] extends AttributeParser [T] {
  def attributes = list.foldLeft(List[Attribute[_]]())(_ ++ _.attributes)
  def parse (m: M): Option[T]
  override def toString = getClass.getName.toString + "(" + list.toString + ")"
}

abstract class Attribute1 [T] extends AttributeSeq[T] {
  def ~ [X] (x: Z[X]) = new Attribute2(this, x)
  def >> [X](fn: T=>X) = new MappedAttributeSeq(this, fn) 
}

class MappedAttributeSeq [A,B](attr: AttributeParser[A], fn: A=>B) extends Attribute1[B] {
  def parse (m: M) = attr.parse(m) map fn
  def list = attr.list
} 

class OptionalAttributeParser[T](parser: AttributeParser[T]) extends Attribute1[Option[T]] {
  def parse (m: M) = Some(parser.parse(m))
  def list = parser.list
}

class LiteralAttributeParser[T](t: T) extends Attribute1[T] {
  def parse (m: M) = Some(t)
  def list = Nil
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
  def ~ [G] (g: Z[G]) = new Attribute7(a,b,c,d,e,f,g)
  def >> [V] (fn: (A,B,C,D,E,F)=>V) = map(fn.tupled)
}
class Attribute7 [A,B,C,D,E,F,G](a:Z[A], b:Z[B], c:Z[C], d:Z[D], e:Z[E], f:Z[F], g:Z[G]) extends AttributeSeq[(A,B,C,D,E,F,G)] {
  def list = List(a,b,c,d,e,f,g)
  def parse (m: M) = for (av <- a parse m; bv <- b parse m; cv <- c parse m; dv <- d parse m; ev <- e parse m; fv <- f parse m; gv <- g parse m) yield (av,bv,cv,dv,ev,fv,gv)
  def ~ [H] (h: Z[H]) = new Attribute8(a,b,c,d,e,f,g,h)
  def >> [V] (fn: (A,B,C,D,E,F,G)=>V) = map(fn.tupled)
}
class Attribute8 [A,B,C,D,E,F,G,H](a:Z[A], b:Z[B], c:Z[C], d:Z[D], e:Z[E], f:Z[F], g:Z[G], h:Z[H]) extends AttributeSeq[(A,B,C,D,E,F,G,H)] {
  def list = List(a,b,c,d,e,f,g,h)
  def parse (m: M) = for (av <- a parse m; bv <- b parse m; cv <- c parse m; dv <- d parse m; ev <- e parse m; fv <- f parse m; gv <- g parse m; hv <- h parse m) yield (av,bv,cv,dv,ev,fv,gv,hv)
  def ~ [I] (i: Z[I]) = new Attribute9(a,b,c,d,e,f,g,h,i)
  def >> [V] (fn: (A,B,C,D,E,F,G,H)=>V) = map(fn.tupled)
}
class Attribute9 [A,B,C,D,E,F,G,H,I](a:Z[A], b:Z[B], c:Z[C], d:Z[D], e:Z[E], f:Z[F], g:Z[G], h:Z[H], i:Z[I]) extends AttributeSeq[(A,B,C,D,E,F,G,H,I)] {
  def list = List(a,b,c,d,e,f,g,h,i)
  def parse (m: M) = for (av <- a parse m; bv <- b parse m; cv <- c parse m; dv <- d parse m; ev <- e parse m; fv <- f parse m; gv <- g parse m; hv <- h parse m; iv <- i parse m) yield (av,bv,cv,dv,ev,fv,gv,hv,iv)
  def ~ [J] (j: Z[J]) = new Attribute10(a,b,c,d,e,f,g,h,i,j)
  def >> [V] (fn: (A,B,C,D,E,F,G,H,I)=>V) = map(fn.tupled)
}
class Attribute10 [A,B,C,D,E,F,G,H,I,J](a:Z[A], b:Z[B], c:Z[C], d:Z[D], e:Z[E], f:Z[F], g:Z[G], h:Z[H], i:Z[I], j:Z[J]) extends AttributeSeq[(A,B,C,D,E,F,G,H,I,J)] {
  def list = List(a,b,c,d,e,f,g,h,i,j)
  def parse (m: M) = for (av <- a parse m; bv <- b parse m; cv <- c parse m; dv <- d parse m; ev <- e parse m; fv <- f parse m; gv <- g parse m; hv <- h parse m; iv <- i parse m; jv <- j parse m) yield (av,bv,cv,dv,ev,fv,gv,hv,iv,jv)
  def ~ [K] (k: Z[K]) = new Attribute11(a,b,c,d,e,f,g,h,i,j,k)
  def >> [V] (fn: (A,B,C,D,E,F,G,H,I,J)=>V) = map(fn.tupled)
}
class Attribute11 [A,B,C,D,E,F,G,H,I,J,K](a:Z[A], b:Z[B], c:Z[C], d:Z[D], e:Z[E], f:Z[F], g:Z[G], h:Z[H], i:Z[I], j:Z[J], k:Z[K]) extends AttributeSeq[(A,B,C,D,E,F,G,H,I,J,K)] {
  def list = List(a,b,c,d,e,f,g,h,i,j,k)
  def parse (m: M) = for (av <- a parse m; bv <- b parse m; cv <- c parse m; dv <- d parse m; ev <- e parse m; fv <- f parse m; gv <- g parse m; hv <- h parse m; iv <- i parse m; jv <- j parse m; kv <- k parse m) yield (av,bv,cv,dv,ev,fv,gv,hv,iv,jv,kv)
  def ~ [L] (l: Z[L]) = new Attribute12(a,b,c,d,e,f,g,h,i,j,k,l)
  def >> [V] (fn: (A,B,C,D,E,F,G,H,I,J,K)=>V) = map(fn.tupled)
}
class Attribute12 [A,B,C,D,E,F,G,H,I,J,K,L](a:Z[A], b:Z[B], c:Z[C], d:Z[D], e:Z[E], f:Z[F], g:Z[G], h:Z[H], i:Z[I], j:Z[J], k:Z[K], l:Z[L]) extends AttributeSeq[(A,B,C,D,E,F,G,H,I,J,K,L)] {
  def list = List(a,b,c,d,e,f,g,h,i,j,k,l)
  def parse (m: M) = for (av <- a parse m; bv <- b parse m; cv <- c parse m; dv <- d parse m; ev <- e parse m; fv <- f parse m; gv <- g parse m; hv <- h parse m; iv <- i parse m; jv <- j parse m; kv <- k parse m; lv <- l parse m) yield (av,bv,cv,dv,ev,fv,gv,hv,iv,jv,kv,lv)
  def ~ [N] (n: Z[N]) = new Attribute13(a,b,c,d,e,f,g,h,i,j,k,l,n)
  def >> [V] (fn: (A,B,C,D,E,F,G,H,I,J,K,L)=>V) = map(fn.tupled)
}
class Attribute13 [A,B,C,D,E,F,G,H,I,J,K,L,N](a:Z[A], b:Z[B], c:Z[C], d:Z[D], e:Z[E], f:Z[F], g:Z[G], h:Z[H], i:Z[I], j:Z[J], k:Z[K], l:Z[L], n:Z[N]) extends AttributeSeq[(A,B,C,D,E,F,G,H,I,J,K,L,N)] {
  def list = List(a,b,c,d,e,f,g,h,i,j,k,l,n)
  def parse (m: M) = for (av <- a parse m; bv <- b parse m; cv <- c parse m; dv <- d parse m; ev <- e parse m; fv <- f parse m; gv <- g parse m; hv <- h parse m; iv <- i parse m; jv <- j parse m; kv <- k parse m; lv <- l parse m; nv <- n parse m) yield (av,bv,cv,dv,ev,fv,gv,hv,iv,jv,kv,lv,nv)
  def ~ [O] (o: Z[O]) = new Attribute14(a,b,c,d,e,f,g,h,i,j,k,l,n,o)
  def >> [V] (fn: (A,B,C,D,E,F,G,H,I,J,K,L,N)=>V) = map(fn.tupled)
}

class Attribute14 [A,B,C,D,E,F,G,H,I,J,K,L,N,O](a:Z[A], b:Z[B], c:Z[C], d:Z[D], e:Z[E], f:Z[F], g:Z[G], h:Z[H], i:Z[I], j:Z[J], k:Z[K], l:Z[L], n:Z[N], o:Z[O]) extends AttributeSeq[(A,B,C,D,E,F,G,H,I,J,K,L,N,O)] {
  def list = List(a,b,c,d,e,f,g,h,i,j,k,l,n,o)
  def parse (m: M) = for (av <- a parse m; bv <- b parse m; cv <- c parse m; dv <- d parse m; ev <- e parse m; fv <- f parse m; gv <- g parse m; hv <- h parse m; iv <- i parse m; jv <- j parse m; kv <- k parse m; lv <- l parse m; nv <- n parse m; ov <- o parse m) yield (av,bv,cv,dv,ev,fv,gv,hv,iv,jv,kv,lv,nv,ov)
  def ~ [P] (p: Z[P]) = new Attribute15(a,b,c,d,e,f,g,h,i,j,k,l,n,o,p)
  def >> [V] (fn: (A,B,C,D,E,F,G,H,I,J,K,L,N,O)=>V) = map(fn.tupled)
}
class Attribute15 [A,B,C,D,E,F,G,H,I,J,K,L,N,O,P](a:Z[A], b:Z[B], c:Z[C], d:Z[D], e:Z[E], f:Z[F], g:Z[G], h:Z[H], i:Z[I], j:Z[J], k:Z[K], l:Z[L], n:Z[N], o:Z[O], p:Z[P]) extends AttributeSeq[(A,B,C,D,E,F,G,H,I,J,K,L,N,O,P)] {
  def list = List(a,b,c,d,e,f,g,h,i,j,k,l,n,o,p)
  def parse (m: M) = for (av <- a parse m; bv <- b parse m; cv <- c parse m; dv <- d parse m; ev <- e parse m; fv <- f parse m; gv <- g parse m; hv <- h parse m; iv <- i parse m; jv <- j parse m; kv <- k parse m; lv <- l parse m; nv <- n parse m; ov <- o parse m; pv <- p parse m) yield (av,bv,cv,dv,ev,fv,gv,hv,iv,jv,kv,lv,nv,ov,pv)
  def ~ [Q] (q: Z[Q]) = new Attribute16(a,b,c,d,e,f,g,h,i,j,k,l,n,o,p,q)
  def >> [V] (fn: (A,B,C,D,E,F,G,H,I,J,K,L,N,O,P)=>V) = map(fn.tupled)
}
class Attribute16 [A,B,C,D,E,F,G,H,I,J,K,L,N,O,P,Q](a:Z[A], b:Z[B], c:Z[C], d:Z[D], e:Z[E], f:Z[F], g:Z[G], h:Z[H], i:Z[I], j:Z[J], k:Z[K], l:Z[L], n:Z[N], o:Z[O], p:Z[P], q:Z[Q]) extends AttributeSeq[(A,B,C,D,E,F,G,H,I,J,K,L,N,O,P,Q)] {
  def list = List(a,b,c,d,e,f,g,h,i,j,k,l,n,o,p,q)
  def parse (m: M) = for (av <- a parse m; bv <- b parse m; cv <- c parse m; dv <- d parse m; ev <- e parse m; fv <- f parse m; gv <- g parse m; hv <- h parse m; iv <- i parse m; jv <- j parse m; kv <- k parse m; lv <- l parse m; nv <- n parse m; ov <- o parse m; pv <- p parse m; qv <- q parse m) yield (av,bv,cv,dv,ev,fv,gv,hv,iv,jv,kv,lv,nv,ov,pv,qv)
  def ~ [R] (r: Z[R]) = new Attribute17(a,b,c,d,e,f,g,h,i,j,k,l,n,o,p,q,r)
  def >> [V] (fn: (A,B,C,D,E,F,G,H,I,J,K,L,N,O,P,Q)=>V) = map(fn.tupled)
}
class Attribute17 [A,B,C,D,E,F,G,H,I,J,K,L,N,O,P,Q,R](a:Z[A], b:Z[B], c:Z[C], d:Z[D], e:Z[E], f:Z[F], g:Z[G], h:Z[H], i:Z[I], j:Z[J], k:Z[K], l:Z[L], n:Z[N], o:Z[O], p:Z[P], q:Z[Q], r:Z[R]) extends AttributeSeq[(A,B,C,D,E,F,G,H,I,J,K,L,N,O,P,Q,R)] {
  def list = List(a,b,c,d,e,f,g,h,i,j,k,l,n,o,p,q,r)
  def parse (m: M) = for (av <- a parse m; bv <- b parse m; cv <- c parse m; dv <- d parse m; ev <- e parse m; fv <- f parse m; gv <- g parse m; hv <- h parse m; iv <- i parse m; jv <- j parse m; kv <- k parse m; lv <- l parse m; nv <- n parse m; ov <- o parse m; pv <- p parse m; qv <- q parse m; rv <- r parse m) yield (av,bv,cv,dv,ev,fv,gv,hv,iv,jv,kv,lv,nv,ov,pv,qv,rv)
  def >> [V] (fn: (A,B,C,D,E,F,G,H,I,J,K,L,N,O,P,Q,R)=>V) = map(fn.tupled)
}

