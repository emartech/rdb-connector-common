package com.emarsys.rdb.connector.common.models

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.DataManipulation.{Record, UpdateDefinition}
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors._
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult

import scala.concurrent.{ExecutionContext, Future}

trait Connector {

  implicit val executionContext: ExecutionContext

  def close(): Future[Unit]

  def testConnection(): ConnectorResponse[Unit]

  def listTables(): ConnectorResponse[Seq[TableModel]]

  def listTablesWithFields(): ConnectorResponse[Seq[FullTableModel]]

  def listFields(table: String): ConnectorResponse[Seq[FieldModel]]

  def isOptimized(table: String, fields: Seq[String]): ConnectorResponse[Boolean]

  def simpleSelect(select: SimpleSelect): ConnectorResponse[Source[Seq[String], NotUsed]]

  def rawSelect(rawSql: String, limit: Option[Int]): ConnectorResponse[Source[Seq[String], NotUsed]]

  def validateRawSelect(rawSql: String): ConnectorResponse[Unit]

  def analyzeRawSelect(rawSql: String): ConnectorResponse[Source[Seq[String], NotUsed]]

  def projectedRawSelect(rawSql: String, fields: Seq[String]): ConnectorResponse[Source[Seq[String], NotUsed]]

  protected def rawUpdate(tableName: String, definitions: Seq[UpdateDefinition]): ConnectorResponse[Int] = ???

  protected def rawInsertData(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = ???

  protected def rawReplaceData(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = ???



  final def update(tableName: String, definitions: Seq[UpdateDefinition]): ConnectorResponse[Int] = {
    ValidateDataManipulation.validateUpdateDefinition(tableName, definitions, this).flatMap {
      case ValidationResult.Valid => rawUpdate(tableName, definitions)
      case validationResult => Future.successful(Left(FailedValidation(validationResult)))
    }
  }

  final def insertIgnore(tableName: String, data: Seq[Record]): ConnectorResponse[Int] = {
    ValidateDataManipulation.validateInsertData(tableName, data, this).flatMap {
      case ValidationResult.Valid => rawInsertData(tableName, data)
      case validationResult => Future.successful(Left(FailedValidation(validationResult)))
    }
  }

  def replaceData(tableName: String, data: Seq[Record]): ConnectorResponse[Int] = {
    ValidateDataManipulation.validateInsertData(tableName, data, this).flatMap {
      case ValidationResult.Valid => rawReplaceData(tableName, data)
      case validationResult => Future.successful(Left(FailedValidation(validationResult)))
    }

  }

}

trait ConnectorCompanion {
  def meta(): MetaData
}