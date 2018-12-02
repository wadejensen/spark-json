package org.apache.spark.sql.catalyst.json

import com.fasterxml.jackson.databind.PropertyNamingStrategy.LowerCaseWithUnderscoresStrategy
import com.wadejensen.sparkjson.test.TestSparkSession
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{MustMatchers, WordSpec}

import scala.util.control.NonFatal

case class Location(lat: Double, lon: Double)
case class Person(
    name: String,   // test String field
    age: Int,       // test Int field
    birthPlace: String, // test snake case field name conversion
    location: Location,   // test nested field
    hobbies: Seq[String], // test array field
    attributes: Map[String, String]   // test map field
)

object PersonJson
    extends SparkInternalJsonParser[Person](new LowerCaseWithUnderscoresStrategy)

@RunWith(classOf[JUnitRunner])
class SparkInternalJsonParserTest extends WordSpec with MustMatchers with TestSparkSession {

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
    age = 23,
    birthPlace = "Brisbane",
    location = Location(
      lat = -33.8688,
      lon = 151.2093
    ),
    hobbies = Seq("programming", "drinking", "climbing"),
    attributes = Map(
      "hair" -> "blonde",
      "eyes" -> "blue"
    )
  )

  "fromJson" must {
    "must deserialize a row in vanilla Scala" in {
      val person = PersonJson.fromJson(personRow)
      println(person)
      person must be (expected)
    }
  }

  "fromJson" must {
    "must deserialize a row as a map function in Spark" in {
      import spark.implicits._
      val personJsonStringDs = spark.createDataset[String](Seq(personRow))
      val personDs = personJsonStringDs.map(PersonJson.fromJson)

      val person = personDs.collect()(0)
      person must be (expected)
    }
  }
  "DataFrameReader.json" must {
    "not match functionality of fromJson" in {
      import spark.implicits._
      val personJsonStringDs = spark.createDataset[String](Seq(personRow))
      val personDF = spark.read.json(personJsonStringDs)

      try {
        val personDs = personDF
          .withColumnRenamed("birth_place", "birthPlace")
          .withColumn("age", col("age").cast(IntegerType) )
          .as[Person]

        val person = personDs.collect()(0)
        person must be (expected)

        fail("Spark has improved. Try native JSON parsing!")
      }
      catch {
        case _: AnalysisException => Unit // Test success
        case NonFatal(_) => fail("Spark has changed some other way. Try native JSON parsing!")
      }
    }
  }
}
