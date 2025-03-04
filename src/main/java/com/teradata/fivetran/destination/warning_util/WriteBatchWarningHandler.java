package com.teradata.fivetran.destination.warning_util;

import fivetran_sdk.v2.Warning;
import fivetran_sdk.v2.WriteBatchResponse;
import io.grpc.stub.StreamObserver;

/**
 * Handles warnings for WriteBatch operations.
 */
public class WriteBatchWarningHandler extends WarningHandler<WriteBatchResponse> {

    /**
     * Constructor for WriteBatchWarningHandler.
     *
     * @param responseObserver The response observer to send warnings to.
     */
    public WriteBatchWarningHandler(StreamObserver<WriteBatchResponse> responseObserver) {
        super(responseObserver);
    }

    /**
     * Creates a WriteBatchResponse containing a warning message.
     *
     * @param message The warning message.
     * @return The WriteBatchResponse with the warning.
     */
    @Override
    public WriteBatchResponse createWarning(String message) {
        return WriteBatchResponse.newBuilder()
                .setWarning(Warning.newBuilder()
                        .setMessage(message)
                        .build())
                .build();
    }
}