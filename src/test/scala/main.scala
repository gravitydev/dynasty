package com.gravitydev.dynasty

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers._
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import data.dynamo._
import scala.concurrent.ExecutionContext.Implicits.global

class DynastySpec extends FlatSpec {
  
  "Basic parsers" should "work" in {
    val data = Map[String,AttributeValue]()

    // parse set
    (tables.UserItem.userId parse data) should be (None)
    (tables.UserItem.workspaces parse data) should be (Some(Set.empty))
  }

  "Get query" should "return" in {
    val dyndb = new AmazonDynamoDBAsyncClient()
    val dyn = Dynasty(dyndb)

    dyn.from(tables.UserItem)
      .where(_.userId === "User", _.userId === "User")
      .select(u => u.userId ~ u.userId)  
  }
  
}

