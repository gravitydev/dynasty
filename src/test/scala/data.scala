package data

import com.gravitydev.dynasty._
import org.joda.time.{DateTime, LocalDate}
import org.joda.time.format.ISODateTimeFormat

object dynamo {
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
  
  object tables {
    object UserItem extends DynamoTable [HashKey[String]] ("users") {
      val userId                  = attr[String]      ("user-id")
      val firstName               = attr[String]      ("first-name")
      val lastName                = attr[String]      ("last-name")
      val email                   = attr[String]      ("email")
      val identities              = attr[Set[String]] ("identities")
      val workspaces              = attr[Set[String]] ("workspaces")
      val createdAt               = attr[DateTime]    ("created-at")
    }
    
    object IdentityItem extends DynamoTable [HashKey[String]]("identities") {
      val scheme                  = attr[String]      ("scheme")
      val key                     = attr[String]      ("key")
      val secret                  = attr[String]      ("secret")
      val userId                  = attr[String]      ("user-id")
    }

    object WorkspaceItem extends DynamoTable [HashKey[String]]("workspaces") {
      val path                    = attr[String]      ("path")
      val name                    = attr[String]      ("name")
      val creatorId               = attr[String]      ("creator-id")
      val createdAt               = attr[DateTime]    ("created-at")
    }
  }
}

