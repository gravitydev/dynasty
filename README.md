Dynasty is a DynamoDB toolkit for scala.

It provides a type-safe, clean, concise alternative to the java API.

Features:
- Custom types (typeclasses)
- Minimizes boilerplate
- Type-safety: designed to catch MANY errors at compile-time, rather than run-time
- Asynchronous (always returns scala futures)
- DSL that supports most commonly used DynamoDB features (batch gets, expectations, conditions, etc)
- Cleaner, easier to use

Current drawback: Some of the newer features of DynamoDB are not available through the DSL yet.

Setup
-----
```scala
package data

import com.gravitydev.dynasty._
import org.joda.time.{DateTime, LocalDate}
import org.joda.time.format.ISODateTimeFormat

object dynamo {
  // aliasing some joda types
  private val isoUtcDateTimeFormat = ISODateTimeFormat.dateTimeNoMillis.withZoneUTC
  private val isoUtcDateFormat = ISODateTimeFormat.date
  
  // custom types
  implicit def dateTimeT = customType[DateTime, String](
    dateStr => isoUtcDateTimeFormat.parseDateTime(dateStr),
    date => isoUtcDateTimeFormat.print(date)
  )
  
  implicit def dateT = customType[LocalDate, String](
    dateStr => isoUtcDateFormat.parseLocalDate(dateStr),
    date => isoUtcDateFormat.print(date)
  )
 
  // tables 
  object tables {

    object UserItem extends DynamoTable [String]("users") {
      val userId              = attr[String]      ("user-id")
      val firstName           = attr[String]      ("first-name")
      val lastName            = attr[String]      ("last-name")
      val email               = attr[String]      ("email")
      val identities          = attr[Set[String]] ("identities")
      val createdAt           = attr[DateTime]    ("created-at")

      def key = userId // hash key
    }
    
    object IdentityItem extends DynamoTable [String]("identities") {
      val identkey            = attr[String]      ("key")
      val scheme              = attr[String]      ("scheme")
      val secret              = attr[String]      ("secret")
      val userId              = attr[String]      ("user-id")

      def key = identKey // hash key
    }

    object UserLogItem extends DynamoTable [(String,DateTime)] ("user-logs") {
      def userId              = attr[String]      ("user-id")
      def createdAt           = attr[DateTime]    ("created-at")
      def content             = attr[String]      ("content")
  
      def key = (userId, createdAt) // hash and range key 
    }
  }
}
```

Usage
-----
```scala
val dyn = Dynasty(...)

// add identity
val createIdent = dyn.put(
  tables.IdentityItem
    .values(
      _.scheme := "openid",
      _.identKey := r.body("key").head,
      _.userId := userId
    )
    .expecting(_.identKey.isAbsent)
)

// add user
val createUser = dyn.put(
  tables.UserItem
    .values(
      _.userId      := userId,
      _.identities  := Set(key),
      _.firstName   := first,
      _.lastName    := last,
      _.email       := email,
      _.createdAt   := DateTime.now
    )
    .expecting(_.userId.isAbsent) // check condition
)

// executing async using Play Framework (for example)
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

// batch get
case class User (firstName: String, lastName: String, email: String)

val res = dyn.batchGet(
  // select multiple items from a user
  tables.UserItem.on( Set("user1", "user2", "user3") )
    .select(u => u.name ~ u.firstName ~ u.lastName ~ u.email >> User.apply), // parse a user
 
  // select multiple items from identity, and do something
  tables.IdentityItem.on( Set("someid", "someid2") )
    .select(i => i.scheme ~ i.userId >> {(scheme, userId) => 
      s"Scheme: ${scheme}, User: ${userId}"
    })
)

```
