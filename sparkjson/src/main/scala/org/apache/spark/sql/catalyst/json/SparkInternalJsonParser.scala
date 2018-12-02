package org.apache.spark.sql.catalyst.json

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import com.fasterxml.jackson.databind.PropertyNamingStrategy.PropertyNamingStrategyBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.annotation.tailrec
import scala.reflect.runtime.universe.TypeTag

/**
  * Provides easy access to Spark's custom wrapper around Jackson JSON parsing which is specialized
  * for working with Scala types, (much better Scala support than what is provided in
  * [[com.fasterxml.jackson.module.scala.DefaultScalaModule]])
  *
  * Based on Spark's public APIs for inferring the schema of a
  * [[org.apache.spark.sql.DataFrame]] or [[org.apache.spark.sql.Dataset]], and then converting
  * between the case class and internal row representations of the data.
  *
  * Only supports JSON encoded as UTF-8 strings.
  *
  * For background information and understanding @see:
  * [[https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-Encoder.html Spark Encoder Link]]
  *
  * [[org.apache.spark.sql.DataFrameReader#json(org.apache.spark.sql.Dataset)]],
  * [[org.apache.spark.sql.catalyst.json.JacksonParser]], and
  * [[com.fasterxml.jackson.core.JsonParser]].
  *
  * @tparam T The type to be parsed from a raw JSON String
  */
abstract class SparkInternalJsonParser[T: TypeTag](
    val namingStrategy: PropertyNamingStrategyBase = new IdenticalStrategy) {

  /**
    * Encoder to map between Spark's [[InternalRow]] representation and the desired POJO [[T]]
    */
  protected val encoder: ExpressionEncoder[T] = ExpressionEncoder[T].resolveAndBind()
  /**
    * The "read schema" for the serialized JSON. Can be used to account for differences in field
    * naming conventions between Java / Scala and the JSON corpus.
    */
  private val readSchema = generateParsingSchema(encoder.schema, namingStrategy)
  /**
    * Low level Jackson parsing options, see
    * [[org.apache.spark.sql.catalyst.json.JSONOptions#setJacksonOptions(com.fasterxml.jackson.core.JsonFactory)]]
    */
  private val opts: JSONOptions = JsonOptionsFactory.create()
  /**
    * Spark's abstraction over Jackson's low level [[JsonFactory]] primitive. Used for mapping
    * JSON strings in to Spark supported data types ([[org.apache.spark.sql.types.DataType]]).
    * This parser operates at a lower level than [[com.fasterxml.jackson.databind.ObjectMapper]].
    */
  private val parser = new JacksonParser(readSchema, opts)

  private val createParser: (JsonFactory, String) => JsonParser =
    (jsonFactory, row) => jsonFactory.createParser(row)

  /**
    * Row-wise mapping of raw JSON string to a Scala case class.
    * [[T]] may be any flat or arbitrarily nested Scala case class or [[Product]], containing only
    * primitive fields or [[Product]]s. Eg. Tuple, Int, Long, String, Option, Seq, Map.
    * (Follows the same rules as [[org.apache.spark.sql.Encoders.product]])
    * Usages where [[T]] does not satisfy these constraints will cause a compilation error.
    */
  def fromJson(row: String): T = {
    val internalRows: Seq[InternalRow] = parser.parse(row, createParser, UTF8String.fromString)
    val internalRow: InternalRow = internalRows match {
      case Nil =>
        throw new java.io.IOException("Failed to parse valid object from json string")
      case Seq(r) =>
        r
      case Seq(r, _*) =>
        /** Spark supports mapping a top level json array into rows in a DataFrame, but that is
          * undesired behaviour in this case.
          * See [[org.apache.spark.sql.catalyst.json.JacksonParser#makeRootConverter]]
          */
        throw new UnsupportedOperationException("Cannot parse a top level JSON array")
    }
    encoder.fromRow(internalRow)
  }

  /**
    * A cheeky hack to generate a new Spark schema (read Schema) to match the property naming scheme
    * of the raw JSON string eg. snake_case, when building the [[InternalRow]] representation.
    * The original schema can still be later used to map the [[InternalRow]] to POJO [[T]],
    * since the order of properties remains the same and [[InternalRow]] is naive of field names.
    */
  private def generateParsingSchema(
      schema: StructType,
      namingStrategy: PropertyNamingStrategyBase): StructType = {

    renameStructFields(schema, namingStrategy.translate)
  }

  /**
    * Recursively walk a [[StructType]] schema in order to generate a new [[StructType]] which is
    * identical, except that with field names a changed to satisfy a given renaming strategy.
    *
    * @param srcSchema  The original [[StructType]] schema
    * @param rename Mapping from POJO field name to serialized JSON field name
    */
  private def renameStructFields(
      srcSchema: StructType,
      rename: String => String): StructType = {

    srcSchema
      .fields
      .foldLeft[StructType](new StructType) { (acc: StructType, srcField: StructField) =>
        srcField.dataType match {
          case nested: StructType =>
            val renamedField = srcField.copy(dataType = renameStructFields(nested, rename))
            acc.add(renamedField)
          case _ =>
            val jsonFieldName = rename(srcField.name)
            acc.add(srcField.copy(name = jsonFieldName))
        }
      }
  }
}
