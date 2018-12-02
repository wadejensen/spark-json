### Spark Internal JSON Parser

A small wrapper for accessing *relatively* public Apache Spark APIs to leverage
Spark's internal [Jackson](https://github.com/FasterXML/jackson)-based JSON 
deserialization code.

After surveying a number of JSON parsing libraries for parsing JSON into
Scala objects / POJOs, I struggled to find a library which was simple to use,
performant, and well integrated with Scala's standard types.

I did however, know that Spark introduced a method `DataFrameReader.json` 
with signature `Dataset[String] => Dataset[T]` in Spark 2.2.

All I really wanted was `String => T`.

But don't worry, I didn't go and write yet another JSON parsing library.

#### Usage

```scala
// Define JSON schema as case classes

case class Location(lat: Double, lon: Double)

case class Person(
    name: String,
    age: Int,
    birthPlace: String,
    location: Location,
    hobbies: Seq[String],
    attributes: Map[String, String]
)

import org.apache.spark.sql.catalyst.json.SparkInternalJsonParser
import com.fasterxml.jackson.databind.PropertyNamingStrategy.LowerCaseWithUnderscoresStrategy

object PersonJsonParser
  extends SparkInternalJsonParser[Person](new LowerCaseWithUnderscoresStrategy)
  
val json =
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

val person = PersonJson.fromJson(json)
// Person(Wade Jensen,23,Brisbane,Location(-33.8688,151.2093),List(programming, drinking, climbing),Map(hair -> blonde, eyes -> blue))

```

#### What's in the box?

* Allows parsing JSON strings into Scala case classes (with native Scala types)
* High performance (based on Jackson)
* Automatically generates schema from any valid case class, or class mixed with `Product`
* Handles any case class, or hierarchy of nested case classes that contain only 
primitive or `Product` fields, ie. :
    * `Int`
    * `Long`
    * `Double`
    * `String`
    * `Option[_]` (nullable fields)
    * `scala.collection.immutable.Seq[_]` (JSON array)
    * `scala.collection.immutable.Map[String, _]` (flat JSON object)
* Supports remapping field names during deserialization according to a given renaming strategy. 
Eg. convert `snake_case` field names to `camelCase` to match Scala variable naming conventions.
 The following strategies already exist:
  * `IdenticalStrategy` (no-op)
  * `LowerCaseWithUnderscoresStrategy`
  * `PascalCaseStrategy`
  * `LowerCaseStrategy`
  * Alternative strategies may be provided by implementing Jackson's 
  `com.fasterxml.jackson.databind.PropertyNamingStrategy.PropertyNamingStrategyBase` interface
* No support at the moment for per field renaming using Jackson's `@JsonProperty` annotions
* **Deserialization only at the moment 😢 (no mapping back from case classes to 
JSON string / byte array representation)**

#### How does it work?
From the docstring on `SparkInternalJsonParser[T]`:
```scala
/**
  * Provides easy access to Spark's custom wrapper around Jackson JSON parsing which is specialized
  * for working with Scala types.
  * 
  * Works by using [[JacksonParser]], Spark's abstraction over Jackson's low level [[JsonFactory]] 
  * primitive. Used for mapping JSON strings into Spark supported data types @see
  * ([[org.apache.spark.sql.types]]) in [[InternalRow]] format.
  * This parser operates at a lower level than [[com.fasterxml.jackson.databind.ObjectMapper]].
  *
  * Once the JSON is parsed, it is stored in Spark's [[InternalRow]] format. We can then retrieve it
  * as a ordinary case class using Spark's public APIs ([[ExpressionEncoder`[T]`]]) for inferring the
  * schema of a [[org.apache.spark.sql.DataFrame]] or [[org.apache.spark.sql.Dataset]], and then 
  * converting between the case class and internal row representations of the data..
  * 
  * The process at a high level:
  * 1) Input row as String
  * 2) [[JacksonParser#`parse[T]`]]
  * 3) [[InternalRow]]
  * 4) [[ExpressionEncoder#fromRow]]
  * 5) Output row as [[T]]
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
    val namingStrategy: PropertyNamingStrategyBase = new IdenticalStrategy)
```

#### Why didn't you choose any other Scala JSON libraries?

As this Stack Overflow poster eloquently points out:
["unfortunately writing a JSON library is the Scala community's version of coding a todo list app.
There are quite a variety of alternatives."](https://stackoverflow.com/questions/8054018/what-json-library-to-use-in-scala/14442630#14442630)

[Another summary of available JSON parsing libraries for Scala, in which the author
finds no clear leader.](https://manuel.bernhardt.io/2015/11/06/a-quick-tour-of-json-libraries-in-scala/)

> Great, now which one to pick?
Honestly I don’t have any good advice here. These days I am sticking to 
play-json and sphere-json, but that’s for no particular reason other than 
these libraries being there and doing what I need to do (parse and write JSON,
traverse and some rare times transform the AST, bind to case classes, make it
possible to support custom Java types). If play-json had support for hierarchies
out of the box I would probably not have even looked for anything else.

##### I've personally worked with:

* **Jackson (from Java and Scala)**

Jackson works very well in Java. Simple, and performant. 
I've struggled to get it to be useful in Scala, having resorted to custom deserializer 
classes and annotations in order to properly read Scala collection types 
(perhaps this is fixed, but I couldn't get it working).

Jackson also makes no affordances for mapping to `Option[_]` for nullable fields
(this also has to be implemented as a custom deserializer with `@JsonDeserialize`, 
[see Jackson Wiki](https://github.com/FasterXML/jackson-module-scala/wiki/FAQ#deserializing-optionint-and-other-primitive-challenges))

The straw that broke the camel's back was when I tried to deserialize a field 
which was a nullable array of strings, ie. `Option[Seq[String]]`.
Even implementing a custom deserializer, Jackson could refused because it couldn't 
guarantee that `JsonDeserializer[Option[Map[_, _]]]` was assignable to 
`Class<JsonDeserializer>`. I think the heavily nested covariant relationships were
too much for it.

* **Circe** (indirectly through [circe-config](https://github.com/circe/circe-config), 
a highly ergonomic Typesafe Config wrapper)
Uses compile time macros (shapeless) to generate parsers from case classes.
Have observed blow-outs in compile times, but still much faster than anything
based on Scalaz.
Confusing error messages.
Requires implicit type evidence in scope for encoding and decoding.

* **DSL-JSON**
Very fast if the benchmarks floating around the internet are to be believed.
Simple to use.
Doesn't support case classes with generic fields ie. type parameters.

* **Play JSON**
I don't want to use a builder / combinator pattern to construct my own
deserializer. Whenever the model drifts, the parser also has to be updated.
Too easy to fix schema problems / schema drift in the parser, rather than
at the source of the data.

### tl;dr
#### [I just want milk that tastes like real milk!](https://www.youtube.com/watch?v=7QphMaa4wxI)


