package com.gravitydev.dynasty

import com.amazonaws.services.dynamodbv2.model._
import scala.collection.JavaConversions._
import scala.language.implicitConversions

class ItemAttribute [T:DynamoType](val name: String) extends Attribute1[T] {
  val mapper = implicitly[DynamoType[T]]

  def list = List(this)

  override def attributes = List(this)
  
  def parse (m: Map[String,AttributeValue]): Option[T] = mapper.extract(m, name)
  
  def += [X](value: X)(implicit ev: Set[X] =:= T) = name -> Seq(new AttributeValueUpdate()
    .withAction(AttributeAction.ADD)
    .withValue(mapper.put(ev(Set(value))).head)) // hackish...

  def -= [X](value: X)(implicit ev: Set[X] =:= T) = name -> Seq(new AttributeValueUpdate()
    .withAction(AttributeAction.DELETE)
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

  def delete = name -> Seq(new AttributeValueUpdate().withAction(AttributeAction.DELETE))
    
  // produce an assignment, let the implicit conversion create an update or a value
  def := (value: T) = new ast.AssignmentTerm(name, mapper.put(value), Seq(mapper.update(value)))

  def :? (value: Option[T]) = value map {v =>
    new ast.AssignmentTerm(name, mapper.put(v), Seq(mapper.update(v)))
  } getOrElse new ast.AssignmentTerm(name, Nil, Nil)
  
  override def toString = "Attribute(name="+name+", mapper="+mapper+")"
 
  @deprecated("Use isNull", "0.2.1")
  def isAbsent = isNull

  def isNull = new ast.UnaryOp(this, ComparisonOperator.NULL) //Seq(name -> new ExpectedAttributeValue().withExists(false))
 
  // comparisons
  def === (v: T) = new ast.ComparisonEquals(this, v)
  def <   (v: T) = new ast.Comparison(this, ComparisonOperator.LT, v)
  def <=  (v: T) = new ast.Comparison(this, ComparisonOperator.LE, v)
  def >   (v: T) = new ast.Comparison(this, ComparisonOperator.GT, v)
  def >=  (v: T) = new ast.Comparison(this, ComparisonOperator.GE, v)

  // TODO: AWS: only works with String or Binary
  def beginsWith [U](v: U)(implicit ev: DynamoType[U] with DynamoUnderlyingType[U]) = new ast.UnderlyingComparison(this, ComparisonOperator.BEGINS_WITH, v)

  def between (a: T, b: T) = new ast.BetweenComparison(this, a, b)
}
