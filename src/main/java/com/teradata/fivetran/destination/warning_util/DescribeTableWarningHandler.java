package com.teradata.fivetran.destination.warning_util;

import fivetran_sdk.v2.DescribeTableResponse;
import fivetran_sdk.v2.Warning;
import io.grpc.stub.StreamObserver;

/**
 * Handles warnings for DescribeTable operations.
 */
public class DescribeTableWarningHandler extends WarningHandler<DescribeTableResponse> {

    /**
     * Constructor for DescribeTableWarningHandler.
     *
     * @param responseObserver The response observer to send warnings to.
     */
    public DescribeTableWarningHandler(StreamObserver<DescribeTableResponse> responseObserver) {
        super(responseObserver);
    }

    /**
     * Creates a DescribeTableResponse containing a warning message.
     *
     * @param message The warning message.
     * @return The DescribeTableResponse with the warning.
     */
    @Override
    public DescribeTableResponse createWarning(String message) {
        return DescribeTableResponse.newBuilder()
                .setWarning(Warning.newBuilder()
                        .setMessage(message)
                        .build())
                .build();
    }
}