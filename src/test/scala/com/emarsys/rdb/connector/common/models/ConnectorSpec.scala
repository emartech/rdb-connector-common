package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.StringValue
import com.emarsys.rdb.connector.common.models.DataManipulation.{Criteria, Record, UpdateDefinition}
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, TableModel}
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult.InvalidOperationOnView
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class ConnectorSpec extends WordSpecLike with Matchers {

  implicit val executionCtx: ExecutionContext = ExecutionContext.global

  val tableName = "tableName"
  val viewName = "viewName"

  val defaultTimeout = 3.seconds

  val myConnector = new Connector {
    override implicit val executionContext: ExecutionContext = executionCtx

    override def close() = ???

    override def listTables() = Future.successful(Right(Seq(TableModel(tableName, false), TableModel(viewName, true))))

    override def listFields(table: String) = Future.successful(Right(Seq(FieldModel("a", ""), FieldModel("b", ""))))

    override def isOptimized(table: String, fields: Seq[String]) = Future.successful(Right(true))

    override protected def rawUpdate(tableName: String, definitions: Seq[DataManipulation.UpdateDefinition]): ConnectorResponse[Int] = Future.successful(Right(4))

    override protected def rawInsertData(tableName: String, data: Seq[Record]): ConnectorResponse[Int] = Future.successful(Right(4))

    override protected def rawReplaceData(tableName: String, data: Seq[Record]): ConnectorResponse[Int] = Future.successful(Right(4))

    override protected def rawUpsert(tableName: String, data: Seq[Record]): ConnectorResponse[Int] = Future.successful(Right(4))

    override protected def rawDelete(tableName: String, criteria: Seq[Criteria]): ConnectorResponse[Int] = Future.successful(Right(4))

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

  "#insert" should {

    "return ok" in {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(myConnector.insertIgnore(tableName, records), defaultTimeout) shouldBe Right(4)
    }

    "return validation error" in {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(myConnector.insertIgnore(viewName, records), defaultTimeout) shouldBe Left(FailedValidation(InvalidOperationOnView))
    }

  }

  "#replace" should {

    "return ok" in {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(myConnector.replaceData(tableName, records), defaultTimeout) shouldBe Right(4)
    }

    "return validation error" in {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(myConnector.replaceData(viewName, records), defaultTimeout) shouldBe Left(FailedValidation(InvalidOperationOnView))
    }
  }

  "#upsert" should {

    "return ok" in {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(myConnector.upsert(tableName, records), defaultTimeout) shouldBe Right(4)
    }

    "return validation error" in {
      val records = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(myConnector.upsert(viewName, records), defaultTimeout) shouldBe Left(FailedValidation(InvalidOperationOnView))
    }

  }

  "#delete" should {

    "return ok" in {
      val criteria = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(myConnector.delete(tableName, criteria), defaultTimeout) shouldBe Right(4)
    }

    "return validation error" in {
      val criteria = Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))
      Await.result(myConnector.delete(viewName, criteria), defaultTimeout) shouldBe Left(FailedValidation(InvalidOperationOnView))
    }
  }

}
