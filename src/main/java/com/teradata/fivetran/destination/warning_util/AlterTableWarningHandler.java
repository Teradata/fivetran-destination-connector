package com.teradata.fivetran.destination.warning_util;

import fivetran_sdk.v2.AlterTableResponse;
import fivetran_sdk.v2.Warning;
import io.grpc.stub.StreamObserver;

public class AlterTableWarningHandler extends WarningHandler {
    StreamObserver<AlterTableResponse> responseObserver;

    public AlterTableWarningHandler(StreamObserver<AlterTableResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    public void handle(String message) {
        super.handle(message);
        responseObserver.onNext(AlterTableResponse.newBuilder()
                .setWarning(Warning.newBuilder()
                        .setMessage(message)
                        .build())
                .build());
    }

    @Override
    public void handle(String message, Throwable t) {
        super.handle(message, t);
        responseObserver.onNext(AlterTableResponse.newBuilder()
                .setWarning(Warning.newBuilder()
                        .setMessage(String.format("%s: %s", message, t.getMessage()))
                        .build())
                .build());
    }
}
