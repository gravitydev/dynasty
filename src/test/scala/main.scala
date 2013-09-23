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

    dyn get (
      tables.UserItem.on("user").select(u => 
        u.userId ~ u.userId
      )
    ) map {x =>
      println(x)
    } onFailure {case ex: Exception =>
      println(ex)
    }

    dyn get (
      tables.EntryItem.on("user" -> "user").select(u => 
        u.entryId ~ u.entryId
      )   
    ) map {x =>
      println(x)
    } onFailure {case ex: Exception =>
      println(ex)
    };

    dyn batchGet (
      tables.UserItem.on(Set("user", "user")).select(u => 
        u.userId ~ u.userId
      ),
      tables.EntryItem.on(Set("test"->"today")).select(e => 
        e.entryId ~ e.createdAt
      )
    ) map {x =>
      println(x)
    } onFailure {case ex: Exception =>
      println(ex)
    }

    dyn put (
      tables.UserItem
        .values(_.userId := "User", _.userId := "User")
        .expecting(_.userId.isAbsent, _.userId.isAbsent)
    )

    dyn.update (
      tables.UserItem.on("user")
        .set(_.userId := "25")
        .expecting(_.userId === "24")
    )
  }
  
}
