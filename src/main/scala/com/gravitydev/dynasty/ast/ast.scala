package com.gravitydev.dynasty
package ast

import com.amazonaws.services.dynamodbv2.model._
import scala.collection.JavaConverters._
import com.gravitydev.dynasty.ItemAttribute

/**
 * Intermediate structure representing a generic assignment
 * Implicit conversions will then turn it into the specific type required based on the context (put, set, etc)
 */
final class AssignmentTerm (val name: String, val put: Seq[AttributeValue], val set: Seq[AttributeValueUpdate])

/**
 * Intermediate structure representing a generic comparison
 * Implicit conversions will then turn it into the specific type required based on the context (expectation, condition, etc)
 */
sealed abstract class ComparisonTerm [T](val attr: ItemAttribute[T], val op: ComparisonOperator, val values: Seq[AttributeValue])

/** Binary expression on 2 values of the same type */
class Comparison[T](attribute: ItemAttribute[T], op: ComparisonOperator, value: T)
  extends ComparisonTerm [T](attribute, op, attribute.mapper.put(value))

class BetweenComparison [T](attribute: ItemAttribute[T], start: T, end: T)
  extends ComparisonTerm [T](attribute, ComparisonOperator.BETWEEN, attribute.mapper.put(start) ++ attribute.mapper.put(end))

/** Binary expression on 2 values of possibly different types */
class UnderlyingComparison[T,U](attribute: ItemAttribute[T], op: ComparisonOperator, value: U)(implicit underlyingTpe: DynamoType[U] with DynamoUnderlyingType[U])
  extends ComparisonTerm [T](attribute, op, underlyingTpe.put(value))

/**
 * Separate type for equals to implicit conversion to an expectation only in the case of equals comparison 
 */
class ComparisonEquals[T](attribute: ItemAttribute[T], value: T) extends Comparison[T](attribute, ComparisonOperator.EQ, value)

class UnaryOp[T](attribute: ItemAttribute[T], op: ComparisonOperator) extends ComparisonTerm[T](attribute, op, Seq())

sealed abstract class ConditionExpr(val condOp: ConditionalOperator, val conditions: Map[String,Condition])

case class SingleConditionExpr (attrName: String, condition: Condition) 
    extends ConditionExpr(ConditionalOperator.AND, Map(attrName -> condition)) {
  def && (that: SingleConditionExpr) = AndConditionExpr( Map( attrName -> condition, that.attrName -> that.condition ) )
  def || (that: SingleConditionExpr) = OrConditionExpr( Map( attrName -> condition, that.attrName -> that.condition ) )
}

case class AndConditionExpr (conds: Map[String,Condition]) extends ConditionExpr(ConditionalOperator.AND, conds) {
  def && (that: SingleConditionExpr) = copy(conds = conds + (that.attrName -> that.condition))
}
case class OrConditionExpr (conds: Map[String,Condition]) extends ConditionExpr(ConditionalOperator.OR, conds) {
  def || (that: SingleConditionExpr) = copy(conds = conds + (that.attrName -> that.condition))
}
