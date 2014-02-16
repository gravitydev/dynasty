package com.gravitydev.dynasty

import com.amazonaws.services.dynamodbv2.model._
import scala.collection.JavaConversions._
import org.slf4j.LoggerFactory

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


/*
  def update [K <: DynamoKey, T <: DynamoTable[K]](table: T, key: K, returnValues: ReturnValue = ReturnValue.NONE)
      (updates: T => (String, Seq[AttributeValueUpdate])*) = logging ("Updating item: "+tablePrefix+table.tableName+" - " + key) {
        */

/*
class SingleHashKeyPredicate
object SingleHashKeyPredicate {
  implicit def from [T<:DynamoTable] (fn: T => KeyValue) = new SingleHashKeyPredicate
}
class SingleCompositeKeyPredicate
object SingleCompositeKeyPredicate {
  implicit def from [T<:DynamoTable] (fn: (T => KeyValue, T => KeyValue)) = new SingleCompositeKeyPredicate
}

class HashKeyPredicate
object HashKeyPredicate {
  implicit def fromFn1 [T<:DynamoTable] (fn: T => Set[KeyValue]) = new HashKeyPredicate
  //implicit def fromFn2 [T<:DynamoTable[_]] (fn: T => (KeyValue, KeyValue)) = new SingleKeyPredicate
}
*/

class QueryBuilder [K,T<:DynamoTable[K]](table: T with DynamoTable[K]) {
  def on (key: K) = new QueryBuilder.WithKey(table, table.key === key)
  def on (keys: Set[K]): QueryBuilder.WithKeys[K,T] = new QueryBuilder.WithKeys(table, table.key in keys)

  /*
  def check (cond: T=>Seq[(String,ExpectedAttributeValue)]*) = new QueryBuilder.WithChecks[K,T](
    table, 
    (cond flatMap (_(table))).toMap[String,ExpectedAttributeValue] 
  )
  */

  def values (values: T => (String, Seq[AttributeValue])*) = PutQuery(
    table,
    values.flatMap {fn =>
      val (key, puts) = fn(table)
      puts.map(key -> _)
    }.toMap[String,AttributeValue]
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
    /*
    def set (changes: T => (String,Seq[AttributeValueUpdate])*) = UpdateQuery(
      table.tableName,
      keys,
      changes.flatMap {fn =>
        val (key, puts) = fn(table)
        puts.map(key -> _)
      }.toMap
    )
    */
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

/*
class PutBuilder [K <: DynamoKey, T <: DynamoTable[K]](dyn: Dynasty, table: T with DynamoTable[K], expected: Option[Map[String, ExpectedAttributeValue]] = None) {
  lazy val logger = LoggerFactory getLogger getClass

  def set (values: T => (String, Seq[AttributeValue])*): PutQuery = {
    val req = new PutItemRequest()
      .withTableName(dyn.tablePrefix + table.tableName)
      .withItem(
        values.flatMap {fn =>
          val (key, puts) = fn(table)
          puts.map(key -> _) 
        }.toMap[String,AttributeValue]
      )

    logger.debug("Put Request: " + req)

    withAsyncHandler[PutItemRequest,PutItemResult] (dyn.client.putItemAsync(expected map {exp => req.withExpected(exp)} getOrElse req, _))
  }
  def expected (expectedValues: (T => Seq[(String, ExpectedAttributeValue)])*) = new PutBuilder(dyn, table, Some(expectedValues.map(_(table)).flatten.toMap))
}
*/

