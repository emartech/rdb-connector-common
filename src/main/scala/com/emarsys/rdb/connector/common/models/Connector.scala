package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.ConnectorResponse

import scala.concurrent.Future

trait Connector {

  def close(): Future[Unit]

  def testConnection(): ConnectorResponse[Unit]

}
