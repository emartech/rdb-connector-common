package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.StringValue
import com.emarsys.rdb.connector.common.models.DataManipulation.UpdateDefinition
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, TableModel}
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult.InvalidOperationOnView
import org.scalatest.{Matchers, WordSpecLike}

import concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class ConnectorSpec extends WordSpecLike with Matchers {

  implicit val executionCtx: ExecutionContext = ExecutionContext.global

  val tableName = "tableName"
  val viewName = "viewName"

  val defaultTimeout = 3.seconds

  val myConnector = new Connector {
    override implicit val executionContext: ExecutionContext = executionCtx

    override def close() = ???

    override def testConnection() = ???

    override def listTables() = Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true))))

    override def listTablesWithFields() = ???

    override def listFields(table: String) = Future.successful(Right(Seq(FieldModel("a", ""), FieldModel("b", ""))))

    override def isOptimized(table: String, fields: Seq[String]) = Future.successful(Right(true))

    override def simpleSelect(select: SimpleSelect) = ???

    override def rawSelect(rawSql: String, limit: Option[Int]) = ???

    override def validateRawSelect(rawSql: String) = ???

    override def analyzeRawSelect(rawSql: String) = ???

    override def projectedRawSelect(rawSql: String, fields: Seq[String]) = ???

    override protected def rawUpdate(tableName: String, definitions: Seq[DataManipulation.UpdateDefinition]): ConnectorResponse[Int] = Future.successful(Right(4))
  }

  "#update" should {

    "return ok" in {
      val definitions = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("b" -> StringValue("2"))))
      Await.result(myConnector.update(tableName, definitions), defaultTimeout) shouldBe Right(4)
    }

    "return validation error" in {
      val definitions = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("b" -> StringValue("2"))))
      Await.result(myConnector.update(viewName, definitions), defaultTimeout) shouldBe Left(FailedValidation(InvalidOperationOnView))
    }

  }

}