package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult

object Errors {

  sealed abstract class ConnectorError(message: String) extends Exception(message)

  case class ErrorWithMessage(message: String) extends ConnectorError(message)

  case class TableNotFound(table: String) extends ConnectorError(s"Table not found: $table")

  case class FailedValidation(validationResult: ValidationResult) extends ConnectorError("Validation Failed")

  case object NotImplementedOperation extends ConnectorError

}
