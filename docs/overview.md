---
name: Teradata Vantage
title: Teradata Vantage Destination connector by Fivetran | Fivetran documentation
description: Easily connect your data sources to Teradata Vantage with Fivetran. Check out our documentation to start syncing your applications, databases, and events today.
hidden: true
---

# Teradata Vantage {% badge text="Partner-Built" /%} {% availabilityBadge connector="teradata_destination" /%}


[Teradata Vantage](https://www.teradata.com/) is a multi-cloud data platform for enterprise analytics, enabling users to solve complex data challenges. You can use Fivetran to ingest data from various sources into the Teradata Vantage destination.

For detailed descriptions of all the components that make Teradata Vantage, see [Teradata Vantage's documentation](https://docs.teradata.com/).

> NOTE: This destination is [partner-built](/docs/partner-built-program). For any questions related to the Teradata Vantage destination connector and its documentation, contact Teradata Vantage by raising an issue in the [Teradata Vantage Fivetran destination GitHub repository](https://github.com/Teradata/fivetran-destination-connector).

----

## Setup guide

Follow our step-by-step [Teradata Vantage setup guide](/docs/destinations/teradata/setup-guide) to connect Teradata Vantage to Fivetran.

----

## Type transformation mapping

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

## Schema changes

| Schema Change          | Supported | Notes                                                                                                                                                                                                                                                                                                                                   |
|------------------------|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Add column                    | ✔       | When Fivetran detects the addition of a column in your source, it automatically adds that column to your Teradata Vantage destination.                                                                                                                                                                                              |
| Change column type            | ✔       | When Fivetran detects a change in the column type in the data source, it automatically changes the column type in your Teradata Vantage destination. To change the column type, Fivetran creates a new column, copies the data from the existing column to the new column, deletes the existing column, and renames the new column. |
| Change primary key or key column type | ✔       | Changing a primary key is not supported in Teradata Vantage. When Fivetran detects a change in a primary key, it creates a new table with the updated primary key, copies the data from the existing table to the new one, deletes the existing table, and renames the new table.                                                                           |
