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
      .withAttributesToGet(query.selector.attributes.map(_.name).distinct asJava)

    val req2 = if (query.consistentRead) req4 else req.withConsistentRead(true)

    logger.debug("GetItem: " + req2)

    withAsyncHandler[GetItemRequest,GetItemResult] (client.getItemAsync(req2, _)) map {x =>
      Option(x.getItem) map {res =>
        val item = res.asScala.toMap
      
        query.selector.parse(item) getOrElse {
          sys.error("Error when parsing [" + query.selector + "] from [" + item + "]")
        }
      }
    }
  }

  def delete [V](query: DeleteQuery): Future[Unit] = {
    val req = new DeleteItemRequest()
      .withTableName(tablePrefix + query.tableName)
      .withKey(query.key.asJava)

    logger.debug("Delete: " + req)

    withAsyncHandler[DeleteItemRequest,DeleteItemResult] (client.deleteItemAsync(req, _)) map {_ => ()}
  }

  def scan [V](query: ScanQuery[V]): Future[List[V]] = {
    val req = new ScanRequest()
      .withTableName(tablePrefix + query.tableName)
      .withAttributesToGet(query.selector.attributes.map(_.name).distinct.asJava)

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
      .withTableName(tablePrefix + query.tableName)
      .withAttributesToGet(query.selector.attributes.map(_.name).distinct.asJava)
      .withKeyConditions(query.predicate.asJava)

    val req2 = query.filter map {x =>
      req
        .withQueryFilter(x.conditions.asJava)
        .withConditionalOperator(x.condOp)
    } getOrElse req

    val req3 = if (query.reverseOrder) req2.withScanIndexForward(false) else req2

    val req4 = query.limit map (lim => req3.withLimit(lim)) getOrElse req3

    val req5 = if (query.consistentRead) req4 else req4.withConsistentRead(true)

    logger.debug("Query: " + req5.toString)

    withAsyncHandler[QueryRequest,QueryResult] (client.queryAsync(req3, _)) map {x =>
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
              .withAttributesToGet(query.selector.attributes.map(_.name).distinct.asJava)
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

