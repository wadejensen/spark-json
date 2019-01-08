package com.wadejensen.jacksonscalaexample

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.twitter.finatra.json.FinatraObjectMapper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{MustMatchers, WordSpec}

case class Location(lat: Double, lon: Double)
case class Person(
    name: String,   // test String field
    age: Option[Int],   // test Int field
    birthPlace: String, // test snake case field name conversion
    location: Location,   // test nested field
    hobbies: Seq[String], // test array field
    attributes: Option[Map[String, String]]   // test map field
)

case class SimplePerson(name: String, age: Int)
case class PersonWithOptionalAge(name: String, maybeAge: Option[Int])
case class PersonWithDefaultAge(name: String, age: Int = 24)
case class CaseClassWithNesting(topLevelField: String, nestedField: Person)
case class CaseClassWithGenericField[T](topLevelField: String, genericField: T)

@RunWith(classOf[JUnitRunner])
class SparkInternalJsonParserTest extends WordSpec with MustMatchers {

  val personRow =
    """{
      |  "name": "Wade Jensen",
      |  "age": 23,
      |  "birth_place": "Brisbane",
      |  "location": {
      |    "lat": -33.8688,
      |    "lon": 151.2093
      |  },
      |  "hobbies": ["programming", "drinking", "climbing"],
      |  "attributes": {
      |    "hair": "blonde",
      |    "eyes": "blue"
      |  }
      |}""".stripMargin

  val expected = Person(
    name = "Wade Jensen",
    age = Some(23),
    birthPlace = "Brisbane",
    location = Location(
      lat = -33.8688,
      lon = 151.2093
    ),
    hobbies = Seq("programming", "drinking", "climbing"),
    attributes = Some(Map(
      "hair" -> "blonde",
      "eyes" -> "blue"
    ))
  )

  "fromJson" must {
    "must deserialize a row in vanilla Scala" in {
      val scalaObjectMapper = FinatraObjectMapper.create()
//      val mapper = new ObjectMapper with ScalaObjectMapper
//      mapper.registerModule(DefaultScalaModule)
//      val scalaObjectMapper = new FinatraObjectMapper(mapper)

      val person = scalaObjectMapper.parse[Person](personRow)
      println(person)
      person must be (expected)
    }
    "parse json into a generic case class hierarchy" in {
      val nestedJson =
        """
          | {
          |   "top_level_field": "my string",
          |   "generic_field": {
          |     "name": "Jack Dorsey",
          |     "age": 24
          |   }
          | }
          | """.stripMargin
      val expected = CaseClassWithGenericField[SimplePerson]("my string", SimplePerson("Jack Dorsey", 24))

      val scalaObjectMapper = FinatraObjectMapper.create()
      val actual = scalaObjectMapper.parse[CaseClassWithGenericField[SimplePerson]](nestedJson)
      Manifest.
      actual must be (expected)
    }
  }
}
