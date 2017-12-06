package com.emarsys.rdb.connector.common.models

object DataManipulation {
  type Record = Map[String, FieldValueWrapper]

  type Criteria = Record

  case class UpdateDefinition(search: Criteria, update: Record)

  case class StringedUpdateDefinition(search: Map[String, String], update: Map[String, String])

  sealed trait FieldValueWrapper

  object FieldValueWrapper {

    case class StringValue(string: String) extends FieldValueWrapper

    case class IntValue(int: Int) extends FieldValueWrapper

    case class BigDecimalValue(number: BigDecimal) extends FieldValueWrapper

    case class BooleanValue(boolean: Boolean) extends FieldValueWrapper

    case object NullValue extends FieldValueWrapper

  }

  def convertUpdateDefinition(definition: UpdateDefinition): StringedUpdateDefinition = {
    StringedUpdateDefinition(convertTypesToString(definition.search), convertTypesToString(definition.update))
  }

  private def convertTypesToString(record: Map[String, FieldValueWrapper]) = {
    record.map({ case (key, value) =>
      val valueAsString = value match {
        case FieldValueWrapper.StringValue(s) => s
        case FieldValueWrapper.IntValue(int) => int.toString
        case FieldValueWrapper.BigDecimalValue(number) => number.toString
        case FieldValueWrapper.BooleanValue(boolean) => if (boolean) "1" else "0"
        case FieldValueWrapper.NullValue => null
      }
      (key, valueAsString)
    })
  }
}
