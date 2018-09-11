package com.emarsys.rdb.connector.common.models

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.notImplementedOperation
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.defaults.{DefaultFieldValueConverters, FieldValueConverters}
import com.emarsys.rdb.connector.common.models.DataManipulation.{Criteria, Record, UpdateDefinition}
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors._
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

trait Connector {

  implicit val executionContext: ExecutionContext
  protected val fieldValueConverters: FieldValueConverters = DefaultFieldValueConverters

  val isErrorRetryable: PartialFunction[Throwable, Boolean] = {
    case _ => false
  }

  def close(): Future[Unit]

  def innerMetrics(): String = {
    "{}"
  }

  def testConnection(): ConnectorResponse[Unit] = notImplementedOperation

  def listTables(): ConnectorResponse[Seq[TableModel]] = notImplementedOperation

  def listTablesWithFields(): ConnectorResponse[Seq[FullTableModel]] = notImplementedOperation

  def listFields(table: String): ConnectorResponse[Seq[FieldModel]] = notImplementedOperation

  def isOptimized(table: String, fields: Seq[String]): ConnectorResponse[Boolean] = notImplementedOperation

  def simpleSelect(select: SimpleSelect, timeout: FiniteDuration): ConnectorResponse[Source[Seq[String], NotUsed]] = notImplementedOperation

  def rawSelect(rawSql: String, limit: Option[Int], timeout: FiniteDuration): ConnectorResponse[Source[Seq[String], NotUsed]] = notImplementedOperation

  def validateRawSelect(rawSql: String): ConnectorResponse[Unit] = notImplementedOperation

  def analyzeRawSelect(rawSql: String): ConnectorResponse[Source[Seq[String], NotUsed]] = notImplementedOperation

  def projectedRawSelect(rawSql: String, fields: Seq[String], limit: Option[Int], timeout: FiniteDuration, allowNullFieldValue: Boolean = false): ConnectorResponse[Source[Seq[String], NotUsed]] = notImplementedOperation

  def validateProjectedRawSelect(rawSql: String, fields: Seq[String]): ConnectorResponse[Unit] = notImplementedOperation

  def rawQuery(rawSql: String, timeout: FiniteDuration): ConnectorResponse[Int] = notImplementedOperation

  protected def rawUpdate(tableName: String, definitions: Seq[UpdateDefinition]): ConnectorResponse[Int] = notImplementedOperation

  protected def rawInsertData(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = notImplementedOperation

  protected def rawReplaceData(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = notImplementedOperation

  protected def rawUpsert(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = notImplementedOperation

  protected def rawDelete(tableName: String, criteria: Seq[Criteria]): ConnectorResponse[Int] = notImplementedOperation

  protected def rawSearch(tableName: String, criteria: Criteria, limit: Option[Int]): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val query = SimpleSelect(
      fields = AllField,
      table  = TableName(tableName),
      where  = Some(createWhereCondition(criteria)),
      limit  = limit
    )

    simpleSelect(query)
  }

  private def createWhereCondition(criteria: Criteria): WhereCondition = {
    import com.emarsys.rdb.connector.common.defaults.FieldValueConverter._
    import fieldValueConverters._

    And(
      criteria
        .mapValues(_.toSimpleSelectValue)
        .map {
          case (field, Some(value)) => EqualToValue(FieldName(field), value)
          case (field, None)        => IsNull(FieldName(field))
        }.toSeq
    )
  }

  final def update(tableName: String, definitions: Seq[UpdateDefinition]): ConnectorResponse[Int] = {
    validateAndExecute(ValidateDataManipulation.validateUpdateDefinition, tableName, rawUpdate, definitions)
  }

  final def insertIgnore(tableName: String, records: Seq[Record]): ConnectorResponse[Int] = {
    validateAndExecute(ValidateDataManipulation.validateInsertData, tableName, rawInsertData, records)
  }

  final def replaceData(tableName: String, records: Seq[Record]): ConnectorResponse[Int] = {
    validateAndExecute(ValidateDataManipulation.validateInsertData, tableName, rawReplaceData, records)
  }

  final def upsert(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = {
    validateAndExecute(ValidateDataManipulation.validateInsertData, tableName, rawUpsert, definitions)
  }

  final def delete(tableName: String, criteria: Seq[Criteria]): ConnectorResponse[Int] = {
    validateAndExecute(ValidateDataManipulation.validateDeleteCriteria, tableName, rawDelete, criteria)
  }

  final def search(tableName: String, criteria: Criteria, limit: Option[Int]): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    ValidateDataManipulation.validateSearchCriteria(tableName, criteria, this).flatMap {
      case ValidationResult.Valid => rawSearch(tableName, criteria, limit)
      case failedValidationResult => Future.successful(Left(FailedValidation(failedValidationResult)))
    }
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
