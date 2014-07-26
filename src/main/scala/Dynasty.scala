package com.gravitydev.dynasty

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import com.typesafe.scalalogging.slf4j.StrictLogging

object Dynasty {
  def apply (client: AmazonDynamoDBAsyncClient, tablePrefix: String = "")(implicit ec: ExecutionContext) = new Dynasty(client, tablePrefix)
}

class Dynasty (
  private [dynasty] val client: AmazonDynamoDBAsyncClient, 
  private [dynasty] val tablePrefix: String
)(implicit ec: ExecutionContext) extends StrictLogging {

  def get [V](query: GetQuery[V]): Future[Option[V]] = {
    val req = new GetItemRequest()
      .withTableName(tablePrefix + query.tableName)
      .withKey(query.key.asJava)
      .withAttributesToGet(query.selector.attributes map (_.name) asJava)

    logger.debug("GetItem: " + req)

    withAsyncHandler[GetItemRequest,GetItemResult] (client.getItemAsync(req, _)) map {x =>
      Option(x.getItem) map {res =>
        val item = res.asScala.toMap
      
        query.selector.parse(item) getOrElse {
          sys.error("Error when parsing [" + query.selector + "] from [" + item + "]")
        }
      }
    }
  }

  def scan [V](query: ScanQuery[V]): Future[List[V]] = {
    val req = new ScanRequest()
      .withTableName(tablePrefix + query.tableName)
      .withAttributesToGet(query.selector.attributes map (_.name) asJava)

    logger.debug("Scan: " + req)

    withAsyncHandler[ScanRequest,ScanResult] (client.scanAsync(req, _)) map {x =>
      x.getItems.asScala.toList map {res =>
        val item = res.asScala.toMap
      
        query.selector.parse(item) getOrElse {
          sys.error("Error when parsing [" + query.selector + "] from [" + item + "]")
        }
      }
    }
  }

  def query [V](query: QueryReq[V]): Future[List[V]] = {
    val req = new QueryRequest()
      .withConditionalOperator(query.predicate.condOp)
      .withKeyConditions(query.predicate.conditions.asJava)

    withAsyncHandler[QueryRequest,QueryResult] (client.queryAsync(req, _)) map {x =>
      x.getItems.asScala.toList map {res =>
        val item = res.asScala.toMap
      
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
              .withKeys(query.keys.map(_.asJava).asJava)
              .withAttributesToGet(query.selector.attributes.map(_.name).asJava)
        }).toMap[String,KeysAndAttributes].asJava
      }

    logger.debug("BatchGetItem: " + req)

    withAsyncHandler [BatchGetItemRequest,BatchGetItemResult](client.batchGetItemAsync(req, _)) map {r =>
      r.getResponses.asScala.toList flatMap {case (k,v) => 
        // find the relevant query
        var parser = queries.find(tablePrefix + _.tableName == k).get.selector

        // parse the attributes
        v.asScala map {item =>
          parser.parse(item.asScala.toMap).getOrElse {
            sys.error("Error when parsing [" + parser + "] from [" + item.asScala.toMap + "]")
          }
        }
      }
    }
  }

  def update (updateQuery: UpdateQuery[_]): Future[Option[Map[String,AttributeValue]]] = {
    val req = new UpdateItemRequest()
      .withTableName(tablePrefix + updateQuery.tableName)
      .withKey(updateQuery.keys.asJava)
      .withAttributeUpdates(
        updateQuery.changes.asJava
      )
      .withReturnValues(updateQuery.returnValues)

    logger.debug("UpdateItem: " + req)

    withAsyncHandler[UpdateItemRequest,UpdateItemResult](client.updateItemAsync(req, _)) map {x =>
      Option(x.getAttributes) map (_.asScala.toMap)
    }

  }
 
  def put (putQuery: PutQuery[_]): Future[PutItemResult] = {
    val req = new PutItemRequest()
      .withTableName(tablePrefix + putQuery.tableName)
      .withItem(
        putQuery.values.asJava
      )

    logger.debug("Put Request: " + req)

    withAsyncHandler[PutItemRequest,PutItemResult] (
      client.putItemAsync(putQuery.expected map {exp => req.withExpected(exp.asJava)} getOrElse req, _)
    )
  }
}
