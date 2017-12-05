package com.emarsys.rdb.connector.common.models

object DataManipulation {
  type Record = Map[String, String]

  type Criteria = Record

  case class UpdateDefinition(search: Map[String, FieldValueWrapper], update: Map[String, FieldValueWrapper])

  case class StringedUpdateDefinition(search: Criteria, update: Record)

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
