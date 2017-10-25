package com.emarsys.rdb.connector

import com.emarsys.rdb.connector.common.models.Errors.ConnectorError

import scala.concurrent.Future

package object common {
  type ConnectorResponse[T] = Future[Either[ConnectorError,T]]
}
