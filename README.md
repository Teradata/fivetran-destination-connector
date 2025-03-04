# Fivetran Teradata Destination

## Pre-requisites
- JDK v17
- Gradle 8

## Retrieving Proto Files
Download the following proto files from [this repository](https://github.com/fivetran/fivetran_sdk) and place them in the `src/main/proto` directory:

- `common.proto`
- `connector_sdk.proto`
- `destination_sdk.proto`

## Steps

1. Build the Jar
```
> gradle jar
```
2. Run the Jar
```
> java -jar build/libs/TeradataDestination.jar
```
