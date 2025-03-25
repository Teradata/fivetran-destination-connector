---
name: Teradata Vantage
title: Teradata Vantage Destination connector by Fivetran | Fivetran documentation
description: Easily connect your data sources to Teradata Vantage with Fivetran. Check out our documentation to start syncing your applications, databases, and events today.
hidden: false
---

# Teradata Vantage {% badge text="Partner-Built" /%} {% badge text="Private Preview" /%}


[Teradata Vantage](https://www.teradata.com/) is the connected multi-cloud data platform for enterprise analytics, enabling users to solve complex data challenges from start to scale. With a modern architecture and built on an open ecosystem, customers worldwide across a range of industries use Vantage to deliver business outcomes and turn data into one of their greatest assets.

For detailed descriptions of all the components, see Teradata Vantage™ User Guide in https://docs.teradata.com/.

You can use Fivetran to ingest data from various sources into Teradata Vantage

> NOTE: This connector is [partner-built](/docs/partner-built-program). For any questions related to Teradata Vantage Destination connector and its documentation, contact Teradata Vantage by raising an issue in the [Teradata Vantage Fivetran Destination Connector](https://github.com/Teradata/fivetran-destination-connector) GitHub repository.

----

## Setup Guide

Follow our step-by-step [Teradata Vantage Setup Guide](/docs/destinations/teradata/setup-guide) to connect Teradata Vantage to Fivetran.

----

## Type Transformation Mapping

When extracting data from your source using Fivetran, Teradata Vantage automatically maps Fivetran data types to its supported data types. If a data type is not supported, it is seamlessly typecast to the nearest compatible Teradata Vantage data type.

The table below demonstrates how Fivetran data types are converted into Teradata Vantage-supported types:

| Fivetran Data Type  | Teradata Vantage Data Type      |
|---------------------|---------------------------------|
| BOOLEAN             | BYTEINT                         |
| SHORT               | SMALLINT                        |
| INT                 | INTEGER                         |
| LONG                | BIGINT                          |
| DECIMAL             | DECIMAL                         |
| FLOAT               | FLOAT                           |
| DOUBLE              | DOUBLE PRECISION                |
| NAIVE_DATE          | DATE                            |
| NAIVE_DATETIME      | TIMESTAMP                       |
| NAIVE_TIME          | TIMESTAMP                       |
| NAIVE_DATE          | TIMESTAMP                       |
| UTC_DATETIME        | TIMESTAMP                       |
| STRING              | VARCHAR                         |
| XML                 | XML                             |
| JSON                | JSON                            |
| BINARY              | BLOB                            |

----

## Schema Changes

| Schema Change          | Supported | Notes                                                                                                                                                                                                                                                                                                                                   |
|------------------------|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Add column                    | ✔       | When Fivetran detects the addition of a column in your source, it automatically adds that column in the Teradata Vantage Destination connector.                                                                                                                                                                                              |
| Change column type            | ✔       | When Fivetran detects a change in the column type in the data source, it automatically changes the column type in the Teradata Vantage Destination connector. To change the column type, Fivetran creates a new column, copies the data from the existing column to the new column, deletes the existing column, and renames the new column. |
| Change key or key column type | ✔       | Changing PRIMARY KEY is not supported in Teradata Vantage. When Fivetran detects a change in a key, it creates a new table with updated PRIMARY KEY, copies the data from the existing table to the new one, deletes the existing table, and renames the new table                                                                           |