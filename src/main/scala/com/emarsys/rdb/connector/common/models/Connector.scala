package com.emarsys.rdb.connector.common.models

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors._

import scala.concurrent.Future

trait Connector {

  def close(): Future[Unit]

  def testConnection(): ConnectorResponse[Unit]

  def listTables(): ConnectorResponse[Seq[TableModel]]

  def listTablesWithFields(): ConnectorResponse[Seq[FullTableModel]]

  def listFields(table: String): ConnectorResponse[Seq[FieldModel]]

  def isOptimized(table: String, fields: Seq[String]): ConnectorResponse[Boolean]

  def simpleSelect(select: SimpleSelect): ConnectorResponse[Source[Seq[String], NotUsed]]

  def rawSelect(rawSql: String, limit: Option[Int]): ConnectorResponse[Source[Seq[String], NotUsed]]

  def validateRawSelect(rawSql: String): ConnectorResponse[Unit]

  def analyzeRawSelect(rawSql: String): ConnectorResponse[Source[Seq[String], NotUsed]] = ???

}

trait ConnectorCompanion {
  def meta(): MetaData
}