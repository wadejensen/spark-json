package org.apache.spark.sql.catalyst.json

import com.fasterxml.jackson.databind.PropertyNamingStrategy.LowerCaseWithUnderscoresStrategy
import com.wadejensen.sparkjson.test.TestSparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{MustMatchers, WordSpec}

case class Location(lat: Double, lon: Double)
case class Person(
    name: String,   // test String field
    age: Int,       // test Int field
    birthPlace: String, // test snake case field name conversion
    location: Location,   // test nested field
    hobbies: Seq[String], // test array field
    attributes: Map[String, String]   // test map field
)

object PersonEventJson
    extends SparkInternalJsonParser[Person](new LowerCaseWithUnderscoresStrategy())

@RunWith(classOf[JUnitRunner])
class SparkInternalJsonParserTest extends WordSpec with MustMatchers with TestSparkSession {

  "fromJson" must {
    "must deserialize a row in vanilla Scala" in {
      val personRow =
        """{
          |  "name": "Wade Jensen",
          |  "age": 23,
          |  "birth_place": "Brisbane",
          |  "location": {
          |    "lat": -33.8688,
          |    "lon": 151.2093
          |  },
          |  "hobbies": ["programming", "drinking", "dancing"],
          |  "attributes": {
          |    "hair": "blonde",
          |    "eyes": "blue"
          |  }
          |}""".stripMargin

      val expected = Person(
        name = "Wade Jensen",
        age = 23,
        birthPlace = "Brisbane",
        location = Location(
          lat = -33.8688,
          lon = 151.2093
        ),
        hobbies = Seq("programming", "drinking", "dancing"),
        attributes = Map(
          "hair" -> "blonde",
          "eyes" -> "blue"
        )
      )

      val person = PersonEventJson.fromJson(personRow)
      person must be (expected)
    }
  }
}


