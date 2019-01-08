package com.wadejensen.jacksonscalaexample

import com.fasterxml.jackson.databind.PropertyNamingStrategy._

/**
  * Strategy for determining the mapping between POJO field names and serialized JSON field names.
  * This strategy requires the serialized JSON to exactly match the POJO field naming scheme,
  * it is the the no-op / identity / do nothing strategy.
  *
  * More useful strategies provided by Jackson include:
  * [[LowerCaseWithUnderscoresStrategy]], [[PascalCaseStrategy]], [[LowerCaseStrategy]]
  */
class IdenticalStrategy extends PropertyNamingStrategyBase {
  override def translate(propertyName: String): String = propertyName
}
