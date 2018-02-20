package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult

object Errors {

  sealed trait ConnectorError

  case class ErrorWithMessage(message: String) extends ConnectorError

  case class TableNotFound(table: String) extends ConnectorError

  case class FailedValidation(validationResult: ValidationResult) extends ConnectorError

  case object NotImplementedOperation extends ConnectorError

}
