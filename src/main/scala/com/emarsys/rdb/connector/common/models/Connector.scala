package com.emarsys.rdb.connector.common.models

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.DataManipulation.{Criteria, Record, UpdateDefinition}
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

  def projectedRawSelect(rawSql: String, fields: Seq[String], allowNullFieldValue: Boolean = false): ConnectorResponse[Source[Seq[String], NotUsed]]

  def validateProjectedRawSelect(rawSql: String, fields: Seq[String]): ConnectorResponse[Unit]

  protected def rawUpdate(tableName: String, definitions: Seq[UpdateDefinition]): ConnectorResponse[Int] = ???

  protected def rawInsertData(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = ???

  protected def rawReplaceData(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = ???

  protected def rawDelete(tableName: String, criteria: Seq[Criteria]): ConnectorResponse[Int] = ???


  final def update(tableName: String, definitions: Seq[UpdateDefinition]): ConnectorResponse[Int] = {
    validateAndExecute(ValidateDataManipulation.validateUpdateDefinition, tableName, rawUpdate, definitions)
  }

  final def insertIgnore(tableName: String, records: Seq[Record]): ConnectorResponse[Int] = {
    validateAndExecute(ValidateDataManipulation.validateInsertData, tableName, rawInsertData, records)
  }

  final def replaceData(tableName: String, records: Seq[Record]): ConnectorResponse[Int] = {
    validateAndExecute(ValidateDataManipulation.validateInsertData, tableName, rawReplaceData, records)
  }

  final def delete(tableName: String, criteria: Seq[Criteria]): ConnectorResponse[Int] = {
    validateAndExecute(ValidateDataManipulation.validateDeleteCriteria, tableName, rawDelete, criteria)
  }

  private def validateAndExecute[T](
                                     validationFn: (String, Seq[T], Connector) => Future[ValidationResult],
                                     tableName:String,
                                     executionFn: (String,Seq[T]) => ConnectorResponse[Int],
                                     data: Seq[T]) = {
    validationFn(tableName, data, this).flatMap {
      case ValidationResult.Valid => executionFn(tableName, data)
      case failedValidationResult => Future.successful(Left(FailedValidation(failedValidationResult)))
    }
  }
}

trait ConnectorCompanion {
  def meta(): MetaData
}
