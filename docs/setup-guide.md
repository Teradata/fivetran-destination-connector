---
name: Teradata Vantage
title: Fivetran for Teradata Vantage | Destination Setup Guide
description: Follow this guide to set up Teradata Vantage as a destination in Fivetran.
---

# Teradata Vantage Setup Guide {% badge text="Partner-Built" /%} {% badge text="Private Preview" /%}

Follow the steps in this guide to connect Teradata Vantage to Fivetran.

> NOTE: This connector is [partner-built](/docs/partner-built-program). For any questions related to Teradata Vantage Destination connector and its documentation, contact Teradata Vantage by raising an issue in the [Teradata Vantage Fivetran Destination Connector](https://github.com/Teradata/fivetran-destination-connector) GitHub repository.

---

## Prerequisites


- A Teradata Vantage instance.

> If you need a test instance of Vantage, you can provision one for free at [https://clearscape.teradata.com](https://clearscape.teradata.com/sign-in?utm_source=dev_portal&utm_medium=quickstart_tutorial&utm_campaign=quickstarts)

- A Fivetran account with the [Create Destinations or Manage Destinations](/docs/using-fivetran/fivetran-dashboard/account-management/role-based-access-control#rbacpermissions) permissions.

---

## Setup Instructions

### <span class="step-item"> Configure Teradata Vantage </span>

Ensure that the Teradata Vantage database user has the following permissions:
* `SELECT`
* `INSERT`
* `UPDATE`
* `DELETE`
* `CREATE`
* `ALTER`
* `CREATE DATABASE` (if `database` configuration is not specified)

### <span class="step-item">Complete Fivetran configuration</span>

1. Log in to your Fivetran account.
2. Go to the [**Destinations** page](https://fivetran.com/dashboard/destinations), and then click **+ Add Destination**.
3. Enter a **Destination name** for your destination, and then click **Add**.
4. Select **Teradata** as the destination type.
5. Enter the following connection configurations for you Teradata Vantage workspace/cluster:
    * **host**
    * **username**
    * **password**
    * **database**
    * **tmode**
    * **logmech**
6. (Optional) Enable SSL and specify related configurations.
7. (Optional) Specify additional **Driver Parameters**. Refer to [https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/frameset.html) documentation for a list of supported parameters. 
8. Click **Save & Test**.

Fivetran tests and validates the Teradata Vantage connection configuration. Once the connection configuration test is successful, you can sync your data using Fivetran connectors to the Teradata Vantage destination.