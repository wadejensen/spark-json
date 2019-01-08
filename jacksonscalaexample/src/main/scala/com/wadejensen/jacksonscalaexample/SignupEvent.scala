//package com.wadejensen.jacksonscalaexample
//
//import com.canva.ds.time.TimeUtil
//import org.apache.spark.sql.Encoders
//import play.api.libs.functional.syntax._
//import play.api.libs.json.{JsPath, Json, Reads}
//
//case class SignupEvent(
//    override val date: Int,
//    override val time: Long,
//    override val eventId: String,
//    override val userId: String,
//    eventType: String,
//    override val context: FirehoseUserProperties,
//    eventProperties: SignupEventProperties) extends FirehoseUserEvent(date, time, eventId, userId, context)
//
//case class SignupEventProperties(
//    userId: String,
//    signupBrandId: String,
//    personalBrandId: Option[String],
//    userRoles: Seq[String],
//    brandRoles: Map[String, String],
//    authProvider: Option[String],
//    referrer: Option[String],
//    journey: Option[String],
//    city: Option[String]
//)
//
//object SignupEvent extends FirehoseParquetEventDefinition[SignupEvent] {
//  override val parquetFileName = "profile_signup_v1.parquet"
//  override val firehoseEventName = "profile_signup"
//  override val encoder = Encoders.product[SignupEvent]
//  override def mapRow(row: String): Option[SignupEvent] = Some(SignupEventJson.fromJson(row))
//}
//
//private object SignupEventJson {
//  def fromJson(json: String): SignupEvent = Json.parse(json).as(SignupEventReads)
//
//  implicit val SignupEventPropertiesReads: Reads[SignupEventProperties] =
//    (
//      (JsPath \ "userId").read[String] and
//        (JsPath \ "signupBrandId").read[String] and
//        (JsPath \ "personalBrandId").readNullable[String] and
//        (JsPath \ "userRoles").read[Seq[String]] and
//        (JsPath \ "brandRoles").read[Map[String, String]] and
//        (JsPath \ "authProvider").readNullable[String] and
//        (JsPath \ "referrer").readNullable[String] and
//        (JsPath \ "journey").readNullable[String] and
//        (JsPath \ "city").readNullable[String]
//    )(SignupEventProperties.apply _)
//
//  private implicit val SignupEventReads: Reads[SignupEvent] =
//    (
//      (JsPath \ "time").read[Long].map(TimeUtil.epochMillisToBasicIsoDateInt(_)) and
//        (JsPath \ "time").read[Long] and
//        (JsPath \ "event_id").read[String] and
//        (JsPath \ "user_id").read[String] and
//        (JsPath \ "event_type").read[String] and
//        (JsPath \ "user_properties")
//            .read[FirehoseUserProperties](FirehoseUserProperties.UserPropertiesReads) and
//        (JsPath \ "event_properties")
//            .read[SignupEventProperties](SignupEventPropertiesReads)
//    )(SignupEvent.apply _)
//}
