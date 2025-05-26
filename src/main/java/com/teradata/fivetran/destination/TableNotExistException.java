package com.teradata.fivetran.destination;

class TableNotExistException extends Exception {
    TableNotExistException(String s) {
        super(String.format("Table: %s doesn't exist", s));
    }
}
