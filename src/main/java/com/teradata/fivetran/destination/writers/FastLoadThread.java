package com.teradata.fivetran.destination.writers;

import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.FileParams;

public class FastLoadThread extends Thread {

    public FastLoad fastLoad;
    String file;
    FileParams params;
    Map<String, ByteString> secretKeys;
    List<Column> columns;

    public FastLoadThread(FastLoad fastLoad, String file, List<Column> columns, FileParams params, Map<String, ByteString> secretKeys) {
        System.out.println("FastLoadThread created");
        this.fastLoad = fastLoad;
        this.file = file;
        this.params = params;
        this.secretKeys = secretKeys;
        this.columns = columns;
    }

    public void run() {
        System.out.println("In FastLoadThread run()");
        try {
            fastLoad.loadData(file, columns, params, secretKeys);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}