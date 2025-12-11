package com.teradata.fivetran.destination.warning_util;

import fivetran_sdk.v2.Warning;
import fivetran_sdk.v2.WriteBatchResponse;
import io.grpc.stub.StreamObserver;

public class WriteBatchWarningHandler extends WarningHandler {
    StreamObserver<WriteBatchResponse> responseObserver;

    public WriteBatchWarningHandler(StreamObserver<WriteBatchResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    public void handle(String message) {
        super.handle(message);
        responseObserver.onNext(createWarning(message));
    }

    @Override
    public void handle(String message, Throwable t) {
        super.handle(message, t);
        responseObserver.onNext(createWarning(String.format("%s: %s", message, t.getMessage())));
    }

    public WriteBatchResponse createWarning(String message) {
        return WriteBatchResponse.newBuilder()
                .setWarning(Warning.newBuilder()
                        .setMessage(message)
                        .build()).build();
    }
}
