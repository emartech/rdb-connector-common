package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage

import scala.concurrent.Future

trait Connector {

  def close(): Future[Unit]

  def testConnection(): Future[Either[ErrorWithMessage, Unit]]

}
