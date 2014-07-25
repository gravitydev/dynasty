package com.gravitydev.dynasty

import com.amazonaws.services.dynamodbv2.model._
import scala.collection.JavaConversions._
import org.slf4j.LoggerFactory

class AssignmentProxy (val name: String, val put: Seq[AttributeValue], val set: Seq[AttributeValueUpdate])

case class GetQuery [V] (
  tableName: String,
  key: Map[String, AttributeValue],
  selector: AttributeSeq[V]
)
case class GetQueryMulti [V](
  tableName: String,
  keys: Seq[Map[String, AttributeValue]],
  selector: AttributeSeq[V]
)

case class ScanQuery [V] (
  tableName: String,
  selector: AttributeSeq[V]
)

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

class QueryBuilder [K,T<:DynamoTable[K]](table: T with DynamoTable[K]) {
  def on (key: K) = new QueryBuilder.WithKey(table, table.key === key)
  def on (keys: Set[K]): QueryBuilder.WithKeys[K,T] = new QueryBuilder.WithKeys(table, table.key in keys)

  def values (values: T => (String, Seq[AttributeValue])*) = PutQuery(
    table,
    values.flatMap {fn =>
      val (key, puts) = fn(table)
      puts.map(key -> _)
    }.toMap[String,AttributeValue]
  )

  def select [V](attributes: T => AttributeSeq[V]): ScanQuery[V] = ScanQuery (
    table.tableName,
    attributes(table)
  )
}

object QueryBuilder {
  class WithKey [K,T<:DynamoTable[K]](table: T with DynamoTable[K], keys: Map[String,AttributeValue]) {
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
}

