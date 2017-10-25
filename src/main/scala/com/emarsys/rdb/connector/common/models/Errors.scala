package com.emarsys.rdb.connector.common.models

object Errors {

  sealed trait ConnectorError

  case class ErrorWithMessage(message: String) extends ConnectorError

}
