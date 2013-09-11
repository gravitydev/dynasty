package com.gravitydev.dynasty

import com.amazonaws.services.dynamodb.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodb.model._
import scala.collection.JavaConversions._
import java.nio.ByteBuffer
import scala.concurrent.{Future, ExecutionContext}
import org.slf4j.LoggerFactory
import com.gravitydev.dynasty.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.{ActorSystem, Scheduler}

object Dynasty {
  def apply (client: AmazonDynamoDBAsyncClient, tablePrefix: String = "")(implicit system: ActorSystem) = 
    new Dynasty(client, system.dispatcher, system.scheduler, tablePrefix)
}

/**
 * Represents a list of items and attributes to be retrieved from *one* table
 * mostly for the purpose of batchGet
 */
case class ItemQuery [K<:DynamoKey, T<:DynamoTable[K], V<:Any](table: T, keys: Set[K])(val attributesFn: T=>AttributeSeq[V]) {
  def tableName = table.tableName
  def dynamoKeys = keys.map(_.key)
  def attributes = attributesFn(table)
}

class Dynasty (
  private [dynasty] val client: AmazonDynamoDBAsyncClient, 
  ec: ExecutionContext, 
  scheduler: Scheduler, 
  private [dynasty] val tablePrefix: String
) {
  implicit val _ec = ec
  implicit val _sc = scheduler
  
  private val logger = LoggerFactory getLogger getClass
  
  def get [K<:DynamoKey, T<:DynamoTable[K], V<:Any]
      (table: T, key: K)(attributes: T => AttributeSeq[V]): Future[Option[V]] = logging ("Getting "+key+" from table "+table.tableName) {
    val attrs = attributes(table)

    val req = new GetItemRequest()
      .withTableName(tablePrefix + table.tableName)
      .withKey(key.key)
      .withAttributesToGet(attrs.attributes map (_.name))

    logger.debug("GetItem: " + req)
    
    client.getItemAsync(req) map {x => 
      Option(x.getItem) map {res =>
        val item = res.toMap
      
        attrs.parse(item).getOrElse {
          sys.error("Error when parsing [" + attrs + "] from [" + item + "]")
        }
      }
    }
  }

  /**
   * @param request (tableName, Seq(keys), Seq(attributes))
   */
  def batchGet [V](queries: ItemQuery[_,_,V]*): Future[List[V]] = logging ("Batch getting: " + queries) {
    val req = new BatchGetItemRequest()
      .withRequestItems {
        (queries map {query =>
          assert(query.dynamoKeys.nonEmpty)
          (tablePrefix + query.tableName) -> 
            new KeysAndAttributes()
              .withKeys(query.dynamoKeys)
              .withAttributesToGet(query.attributes.attributes.map(_.name))
        }).toMap[String,KeysAndAttributes]
      }

    logger.debug("BatchGetItem: " + req)

    client.batchGetItemAsync {
      req
    } map {r => 
      r.getResponses.toList flatMap {case (k,v) => 
        // find the relevant query
        var parser = queries.find(tablePrefix + _.tableName == k).get.attributes

        // parse the attributes
        v.getItems().map {item =>
          parser.parse(item.toMap).getOrElse {
            sys.error("Error when parsing [" + parser + "] from [" + item.toMap + "]")
          }
        }
      }
    }
  }
  
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

    client.updateItemAsync(
      req
    ) map {x =>
      logger.debug("returnValues: " + returnValues)
      logger.debug("RETURNED: " + x)
      
      Option(x.getAttributes) map (_.toMap)
    }
  }
  
  def put [K <: DynamoKey, T <: DynamoTable[K]](table: T with DynamoTable[K]) = new PutBuilder(this, table)
  
  private [dynasty] def exec [T](fn: AmazonDynamoDBAsyncClient => java.util.concurrent.Future[T]): Future[T] = fn(client)
}

class PutBuilder [K <: DynamoKey, T <: DynamoTable[K]](dyn: Dynasty, table: T with DynamoTable[K], expected: Option[Map[String, ExpectedAttributeValue]] = None) {
  lazy val logger = LoggerFactory getLogger getClass

  def set (values: T => (String, Seq[AttributeValue])*): Future[PutItemResult] = dyn.exec {client =>
    val req = new PutItemRequest()
      .withTableName(dyn.tablePrefix + table.tableName)
      .withItem(
        values.flatMap {fn =>
          val (key, puts) = fn(table)
          puts.map(key -> _) 
        }.toMap[String,AttributeValue]
      )

    logger.debug("Put Request: " + req)
    
    client.putItemAsync(
      expected map {exp => req.withExpected(exp)} getOrElse req
    )
  }
  def expected (expectedValues: (T => Seq[(String, ExpectedAttributeValue)])*) = new PutBuilder(dyn, table, Some(expectedValues.map(_(table)).flatten.toMap))
}
