package org.apache.spark.sql.catalyst.json

import java.lang.reflect.Constructor

import com.fasterxml.jackson.core.JsonParser

object JsonOptionsFactory {

  /**
    * Sneakily create an instance of [[org.apache.spark.sql.catalyst.json.JSONOptions]] with
    * reflection, because its the only private API required to use Spark's Jackson-based JSON
    * parsing.
    *
    * Because of the dodgey reflection being done, we very carefully check the constructor, and
    * informatively throw if the private API has changed.
    *
    * It is critical that this class be located in the same package as [[JSONOptions]]
    * within Apache Spark, to fool the Scala compiler into allowing access to the [[JSONOptions]]
    * symbol.
    *
    * @param parameters Options for parsing JSON data into Spark SQL rows.
    *                   Most of these map directly to Jackson's internal options, specified in
    *                   [[JsonParser.Feature]]
    */
  def create(
      parameters: Map[String, String] = Map.empty[String, String],
      defaultTimeZoneId: String = "UTC",
      defaultColumnNameOfCorruptRecord: String = ""): JSONOptions = {

    val constructors: Seq[Constructor[_]] = classOf[JSONOptions]
        .getConstructors
        .toSeq

    val secondaryCtor = constructors
        .filter { ctor =>
          isDesiredSecondaryConstructor(
            ctor,
            parameters,
            defaultTimeZoneId,
            defaultColumnNameOfCorruptRecord)
        }

    secondaryCtor match {
      case Seq(ctor) =>
        ctor
        .newInstance(parameters, defaultTimeZoneId, defaultColumnNameOfCorruptRecord)
        .asInstanceOf[JSONOptions]
      case Seq(ctor, _*) => throw new IllegalStateException(
        "Cannot have multiple constructors with the same signature")
      case Nil => throw new InstantiationException(
        s"""Cannot find constructor of signature:
          | public org.apache.spark.sql.catalyst.json.JSONOptions(
          |   ${parameters.getClass},
          |   ${defaultTimeZoneId.getClass},
          |   ${defaultColumnNameOfCorruptRecord.getClass})
        """.stripMargin)

    }
  }

  /**
    * Return true iff a [[JSONOptions]] constructor matches the desired signature:
    * public org.apache.spark.sql.catalyst.json.JSONOptions(
    *   scala.collection.immutable.Map,
    *   java.lang.String,
    *   java.lang.String
    * )
    */
  def isDesiredSecondaryConstructor(
      ctor: Constructor[_],
      parameters: Map[String, String],
      defaultTimeZoneId: String,
      defaultColumnNameOfCorruptRecord: String): Boolean = {

    val ctorParams: Array[Class[_]] = ctor.getParameterTypes
    ctorParams.length == 3 &&
        ctorParams(0).isAssignableFrom(parameters.getClass) &&
        ctorParams(1).isAssignableFrom(defaultTimeZoneId.getClass) &&
        ctorParams(2).isAssignableFrom(defaultColumnNameOfCorruptRecord.getClass)
  }
}
