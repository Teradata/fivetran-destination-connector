package com.teradata.fivetran.destination.warning_util;

import fivetran_sdk.v2.MigrateResponse;
import fivetran_sdk.v2.Warning;
import io.grpc.stub.StreamObserver;

public class MigrateWarningHandler extends WarningHandler {
    StreamObserver<MigrateResponse> responseObserver;

    public MigrateWarningHandler(StreamObserver<MigrateResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    public void handle(String message) {
        super.handle(message);
        responseObserver.onNext(MigrateResponse.newBuilder()
                .setWarning(Warning.newBuilder()
                        .setMessage(message)
                        .build())
                .build());
    }

    @Override
    public void handle(String message, Throwable t) {
        super.handle(message, t);
        responseObserver.onNext(MigrateResponse.newBuilder()
                .setWarning(Warning.newBuilder()
                        .setMessage(String.format("%s: %s", message, t.getMessage()))
                        .build())
                .build());
    }
}
