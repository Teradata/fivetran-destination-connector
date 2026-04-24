package com.teradata.fivetran.destination.warning_util;

import fivetran_sdk.v2.DescribeTableResponse;
import fivetran_sdk.v2.Warning;
import io.grpc.stub.StreamObserver;

public class DescribeTableWarningHandler extends WarningHandler {
    StreamObserver<DescribeTableResponse> responseObserver;

    public DescribeTableWarningHandler(StreamObserver<DescribeTableResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    public void handle(String message) {
        super.handle(message);
        responseObserver.onNext(DescribeTableResponse.newBuilder()
                .setWarning(Warning.newBuilder()
                        .setMessage(message)
                        .build())
                .build());
    }

    @Override
    public void handle(String message, Throwable t) {
        super.handle(message, t);
        responseObserver.onNext(DescribeTableResponse.newBuilder()
                .setWarning(Warning.newBuilder()
                        .setMessage(String.format("%s: %s", message, t.getMessage()))
                        .build())
                .build());
    }
}
