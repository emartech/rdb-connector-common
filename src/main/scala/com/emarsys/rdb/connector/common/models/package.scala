package com.emarsys.rdb.connector.common

package object models {

  trait ConnectionConfig

  case class MetaData(nameQuoter: String, valueQuoter: String, escape: String)

}
