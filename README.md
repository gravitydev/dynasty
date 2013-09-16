Dynasty is a DynamoDB toolkit for scala

Setup
-----
```scala
package data

import com.gravitydev.dynasty._
import org.joda.time.{DateTime, LocalDate}
import org.joda.time.format.ISODateTimeFormat

object dynamo {
  // custom types
  private val isoUtcDateTimeFormat = ISODateTimeFormat.dateTimeNoMillis.withZoneUTC
  private val isoUtcDateFormat = ISODateTimeFormat.date
  
  implicit def dateTimeT = new DynamoValueMapper[DateTime, String](StringType)(
    dateStr => isoUtcDateTimeFormat.parseDateTime(dateStr),
    date => isoUtcDateTimeFormat.print(date)
  )
  
  implicit def dateT = new DynamoValueMapper[LocalDate, String](StringType)(
    dateStr => isoUtcDateFormat.parseLocalDate(dateStr),
    date => isoUtcDateFormat.print(date)
  )
 
  // tables 
  object tables {
    object UserItem extends DynamoTable [HashKey[String]]("users") {
      val userId                  = attr[String]      ("user-id")
      val firstName               = attr[String]      ("first-name")
      val lastName                = attr[String]      ("last-name")
      val email                   = attr[String]      ("email")
      val identities              = attr[Set[String]] ("identities")
      val createdAt               = attr[DateTime]    ("created-at")
    }
    
    object IdentityItem extends DynamoTable [HashKey[String]]("identities") {
      val scheme                  = attr[String]      ("scheme")
      val key                     = attr[String]      ("key")
      val secret                  = attr[String]      ("secret")
      val userId                  = attr[String]      ("user-id")
    }
  }
}
```

Usage
-----
```scala
// add identity
val createIdent = core.Context.dyn.put(tables.IdentityItem)
  .expected(_.key.exists(false))
  .set(
    _.scheme := "openid",
    _.key := r.body("key").head,
    _.userId := userId
  )

// add user
val createUser = core.Context.dyn.put(tables.UserItem)
  .expected(_.userId.exists(false))
  .set(
    _.userId      := userId,
    _.identities  := Set(key),
    _.firstName   := first,
    _.lastName    := last,
    _.email       := email,
    _.createdAt   := DateTime.now
  )

Async {
  for {
    _ <- createIdent
    _ <- createUser
  } yield Redirect(routes.Application.index).withSession(
    "user-id" -> userId,
    "user-email" -> email,
    "user-name" -> (first + " " + last)
  )
}
```
