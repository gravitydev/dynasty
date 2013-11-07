package com.gravitydev.dynasty

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import java.nio.ByteBuffer
import scala.concurrent.Future
import org.slf4j.LoggerFactory

object Dynasty {
  def apply (client: AmazonDynamoDBAsyncClient, tablePrefix: String = "")(implicit ec: ExecutionContext) = new Dynasty(client, tablePrefix)
}

class KeyValue (key: String, value: AttributeValue) {
  def toPair = key -> value
  def toMap = Map(key -> value)
}
class KeyValues (key: String, values: Set[AttributeValue]) {
  //def toMap = Map( 
}

class Dynasty (
  private [dynasty] val client: AmazonDynamoDBAsyncClient, 
  private [dynasty] val tablePrefix: String
)(implicit ec: ExecutionContext) {
  
  private val logger = LoggerFactory getLogger getClass

  def get [V](query: GetQuery[V]): Future[Option[V]] = {
    val req = new GetItemRequest()
      .withTableName(tablePrefix + query.tableName)
      .withKey(query.key)
      .withAttributesToGet(query.selector.attributes map (_.name))

    logger.debug("GetItem: " + req)

    withAsyncHandler[GetItemRequest,GetItemResult] (client.getItemAsync(req, _)) map {x =>
      Option(x.getItem) map {res =>
        val item = res.toMap
      
        query.selector.parse(item) getOrElse {
          sys.error("Error when parsing [" + query.selector + "] from [" + item + "]")
        }
      }
    }
  }

  /**
   * @param request (tableName, Seq(keys), Seq(attributes))
   */
  def batchGet [V](queries: GetQueryMulti[V]*): Future[List[V]] = logging ("Batch getting: " + queries) {
    val req = new BatchGetItemRequest()
      .withRequestItems {
        (queries.filter(_.keys.nonEmpty) map {query =>
          (tablePrefix+query.tableName) -> 
            new KeysAndAttributes()
              .withKeys(query.keys.map(_.asJava))
              .withAttributesToGet(query.selector.attributes.map(_.name))
        }).toMap[String,KeysAndAttributes]
      }

    logger.debug("BatchGetItem: " + req)

    withAsyncHandler [BatchGetItemRequest,BatchGetItemResult](client.batchGetItemAsync(req, _)) map {r =>
      r.getResponses.toList flatMap {case (k,v) => 
        // find the relevant query
        var parser = queries.find(tablePrefix + _.tableName == k).get.selector

        // parse the attributes
        v map {item =>
          parser.parse(item.toMap).getOrElse {
            sys.error("Error when parsing [" + parser + "] from [" + item.toMap + "]")
          }
        }
      }
    }
  }

  def update (updateQuery: UpdateQuery[_]): Future[Option[Map[String,AttributeValue]]] = {
    val req = new UpdateItemRequest()
      .withTableName(tablePrefix + updateQuery.tableName)
      .withKey(updateQuery.keys)
      .withAttributeUpdates {
        updateQuery.changes
        /*
        updateQuery.changes.flatMap {fn => 
          val (key, updates) = fn(table)
          updates.map(key -> _)
        }.toMap[String,AttributeValueUpdate]
        */
      }
      .withReturnValues(updateQuery.returnValues)

    logger.debug("UpdateItem: " + req)

    withAsyncHandler[UpdateItemRequest,UpdateItemResult](client.updateItemAsync(req, _)) map {x =>
      //logger.debug("returnValues: " + returnValues)
      //logger.debug("RETURNED: " + x)
      
      Option(x.getAttributes) map (_.toMap)
    }

  }
 
  /*
  def update [K <: DynamoKey, T <: DynamoTable[K]](table: T, key: K, returnValues: ReturnValue = ReturnValue.NONE)
      (updates: T => (String, Seq[AttributeValueUpdate])*) = logging ("Updating item: "+tablePrefix+table.tableName+" - " + key) {

    val req = new UpdateItemRequest()
      .withTableName(tablePrefix + table.tableName)
      .withKey(key.key)
      .withAttributeUpdates {
        updates.flatMap {fn => 
          val (key, updates) = fn(table)
          updates.map(key -> _)
        }.toMap[String,AttributeValueUpdate]
      }
      .withReturnValues(returnValues)

    logger.debug("UpdateItem: " + req)

    withAsyncHandler[UpdateItemRequest,UpdateItemResult](client.updateItemAsync(req, _)) map {x =>
      logger.debug("returnValues: " + returnValues)
      logger.debug("RETURNED: " + x)
      
      Option(x.getAttributes) map (_.toMap)
    }
  }
  */
  
  def put (putQuery: PutQuery[_]): Future[PutItemResult] = {
    val req = new PutItemRequest()
      .withTableName(tablePrefix + putQuery.tableName)
      .withItem(
        putQuery.values
      )

    logger.debug("Put Request: " + req)

    withAsyncHandler[PutItemRequest,PutItemResult] (client.putItemAsync(putQuery.expected map {exp => req.withExpected(exp)} getOrElse req, _))
  }
}

/*
class PutBuilder [K <: DynamoKey, T <: DynamoTable[K]](dyn: Dynasty, table: T with DynamoTable[K], expected: Option[Map[String, ExpectedAttributeValue]] = None) {
  lazy val logger = LoggerFactory getLogger getClass

  def set (values: T => (String, Seq[AttributeValue])*): Future[PutItemResult] = {
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

