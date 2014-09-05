package com.gravitydev.dynasty

import com.amazonaws.services.dynamodbv2.model._
import scala.collection.JavaConverters._

/**
 * Intermediate structure representing a generic assignment
 * Implicit conversions will then turn it into the specific type required based on the context (put, set, etc)
 */
final class AssignmentTerm (val name: String, val put: Seq[AttributeValue], val set: Seq[AttributeValueUpdate])

/**
 * Intermediate structure representing a generic comparison
 * Implicit conversions will then turn it into the specific type required based on the context (expectation, condition, etc)
 */
sealed abstract class ComparisonTerm [T](val attr: Attribute[T], val op: ComparisonOperator, val values: Seq[AttributeValue])

class Comparison[T](attribute: Attribute[T], op: ComparisonOperator, value: T)
  extends ComparisonTerm [T](attribute, op, attribute.mapper.put(value))

class BetweenComparison [T](attribute: Attribute[T], start: T, end: T)
  extends ComparisonTerm [T](attribute, ComparisonOperator.BETWEEN, attribute.mapper.put(start) ++ attribute.mapper.put(end))

/**
 * Separate type for equals to implicit conversion to an expectation only in the case of equals comparison 
 */
class ComparisonEquals[T](attribute: Attribute[T], value: T) extends Comparison[T](attribute, ComparisonOperator.EQ, value)

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

final case class GetQuery [V] (
  tableName: String,
  key: Map[String, AttributeValue],
  selector: AttributeSeq[V],
  consistentRead: Boolean = false
) {
  def consistent = copy(consistentRead = true)
  def consistent(consistentRead: Boolean) = copy(consistentRead = consistentRead)
}
final case class GetQueryMulti [V](
  tableName: String,
  keys: Seq[Map[String, AttributeValue]],
  selector: AttributeSeq[V]
)

case class ScanQuery [V] (
  tableName: String,
  selector: AttributeSeq[V]
)

case class QueryReq [V](
  tableName: String,
  predicate: Map[String,Condition],
  selector: AttributeSeq[V],
  filter: Option[ConditionExpr],
  limit: Option[Int] = None,
  reverseOrder: Boolean = false,
  consistentRead: Boolean = false,
  indexName: Option[String] = None
) {
  def reverse = copy(reverseOrder = true)
  def limit (num: Int) = copy(limit = Some(num))
  def consistent = copy(consistentRead = true)
  def consistent(consistentRead: Boolean) = copy(consistentRead = consistentRead)
}

case class PutQuery [T<:DynamoTable[_]] (
  table: T,
  values: Map[String, AttributeValue],
  expected: Option[Map[String, ExpectedAttributeValue]] = None
) {
  def tableName = table.tableName
  def expecting (conditions: T=>Seq[(String,ExpectedAttributeValue)]*) = copy(expected = Some(conditions.flatMap(_(table)).toMap))
}

case class UpdateQuery [T<:DynamoTable[_]] (
  table: T,
  keys: Map[String,AttributeValue],
  changes: Map[String, AttributeValueUpdate],
  expected: Option[Map[String, ExpectedAttributeValue]] = None,
  returnValues: ReturnValue = ReturnValue.NONE
) {
  def tableName = table.tableName
  def returning (returnValues: ReturnValue) = copy(returnValues = returnValues)
  def expecting (conditions: T=>Seq[(String,ExpectedAttributeValue)]*) = copy(expected = Some(conditions.flatMap(_(table)).toMap))
}

final case class DeleteQuery (
  tableName: String,
  key: Map[String, AttributeValue] 
)
object DeleteQuery {
  implicit def fromQueryBuilderWithKeys(v: QueryBuilder.WithKey[_,_]) = DeleteQuery(v.table.tableName, v.keys)
}


/**
 * DSL for building dynamodb requests
 */
class QueryBuilder [K,T<:DynamoTable[K]](table: T with DynamoTable[K], _index: Option[DynamoIndex[T]] = None) {
  /** Build 'get' queries */
  def on (key: K) = new QueryBuilder.WithKey(table, table.key === key)

  /** Build 'batch get' queries */
  def on (keys: Set[K]): QueryBuilder.WithKeys[K,T] = new QueryBuilder.WithKeys(table, table.key in keys)

  /** Build 'put' queries */
  def values (values: T => (String, Seq[AttributeValue])*) = PutQuery(
    table,
    values.flatMap {fn =>
      val (key, puts) = fn(table)
      puts.map(key -> _)
    }.toMap[String,AttributeValue]
  )

  def index (indexFn: T => DynamoIndex[T]) = new QueryBuilder(table, _index = Some(indexFn(table)))

  def where (conditions: T => SingleConditionExpr*): QueryBuilder.WithPredicate[K,T] = new QueryBuilder.WithPredicate(
    table, 
    conditions map {cond => 
      val x = cond(table)
      x.attrName -> x.condition
    } toMap,
    _index = _index
  )

  /** Build 'scan' request */
  def select [V](attributes: T => AttributeSeq[V]): ScanQuery[V] = ScanQuery (
    table.tableName,
    attributes(table)
  )
}

object QueryBuilder {
  class WithKey [K,T<:DynamoTable[K]](val table: T with DynamoTable[K], val keys: Map[String,AttributeValue]) {
    def select [V](attributes: T => AttributeSeq[V]): GetQuery[V] = GetQuery (
      table.tableName,
      keys,
      attributes(table)
    )
    def set (changes: T => (String,Seq[AttributeValueUpdate])*) = UpdateQuery(
      table,
      keys,
      changes.flatMap {fn =>
        val (key, puts) = fn(table)
        puts.map(key -> _)
      }.toMap
    )
  }
  class WithKeys [K,T<:DynamoTable[K]](table: T with DynamoTable[K], keys: Seq[Map[String,AttributeValue]]) {
    def select [V](attributes: T => AttributeSeq[V]): GetQueryMulti[V] = GetQueryMulti (
      table.tableName,
      keys,
      attributes(table)
    )
  }
  class WithChecks [K,T<:DynamoTable[K]](table: T, expected: Map[String,ExpectedAttributeValue]) {
    def values (values: T => (String, Seq[AttributeValue])*) = PutQuery(
      table,
      values.flatMap {fn =>
        val (key, puts) = fn(table)
        puts.map(key -> _)
      }.toMap[String,AttributeValue],
      Some(expected)
    )
  }
  class WithPredicate [K,T<:DynamoTable[K]](
    table: T, 
    predicate: Map[String,Condition], 
    _index: Option[DynamoIndex[T]],
    filter: Option[ConditionExpr] = None
  ) {
    /** Build 'query' request */
    def filter (filterFn: T => ConditionExpr) = new QueryBuilder.WithPredicate[K,T](table, predicate, _index, Some(filterFn(table)))

    def index (indexFn: T => DynamoIndex[T]) = new WithPredicate[K,T](table, predicate, _index = Some(indexFn(table)), filter)

    def select [V](attributes: T => AttributeSeq[V]): QueryReq[V] = QueryReq[V](
      table.tableName,
      predicate,
      attributes(table),
      filter,
      indexName = _index.map(_.name)
    ) 
  }
}

