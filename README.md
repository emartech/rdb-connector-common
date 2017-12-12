# Rdb - connector - common
[ ![Codeship Status for emartech/rdb-connector-common](https://app.codeship.com/projects/0178d870-9c53-0135-6d81-1a351be8063b/status?branch=master)](https://app.codeship.com/projects/252925)
[![](https://www.jitpack.io/v/emartech/rdb-connector-common.svg)](https://www.jitpack.io/#emartech/rdb-connector-common)

## Definitions:

**Router** - instantiates a specific type of connector
 
**Database connector** - implements an interface, so that the router can be connected to the specific type of database.

## Tasks:

Defines a Connector trait, that every connector should implement. Contains the common logic, case classes and some default implementations, that may be overwritten by specific connectors, and may use functions implemented in the connectors. (eg. validation)

## Dependants:

**[Rdb - connector - test](https://github.com/emartech/rdb-connector-test)**  - contains common test implementations, that may be used in specific connectors



**[Rdb - connector - redshift](https://github.com/emartech/rdb-connector-redshift)** - implements the Connector trait from the common connector, and contains the redshift scpecific implementation. For testing, it uses the tests written in rdb - conn - common - spec, applied for the redshift connector.

**[Rdb - connector - mysql](https://github.com/emartech/rdb-connector-mysql)** -implements the Connector trait from the common connector, and contains the mysql
 scpecific implementation. For testing, it uses the tests written in rdb - conn - common - spec, applied for the mysql connector.

