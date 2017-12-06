package com.emarsys.rdb.connector.common.defaults

import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper

trait FieldValueWrapperConverter {
  def convertTypesToString(value: FieldValueWrapper): String
}

object DefaultFieldValueWrapperConverter extends FieldValueWrapperConverter {
  def convertTypesToString(value: FieldValueWrapper): String = {
    value match {
      case FieldValueWrapper.StringValue(s) => s
      case FieldValueWrapper.IntValue(int) => int.toString
      case FieldValueWrapper.BigDecimalValue(number) => number.toString
      case FieldValueWrapper.BooleanValue(boolean) => if (boolean) "1" else "0"
      case FieldValueWrapper.NullValue => null
    }
  }
}