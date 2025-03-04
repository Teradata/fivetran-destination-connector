package com.teradata.fivetran.destination.warning_util;

import fivetran_sdk.v2.AlterTableResponse;
import fivetran_sdk.v2.Warning;
import io.grpc.stub.StreamObserver;

/**
 * Handles warnings for AlterTable operations.
 */
public class AlterTableWarningHandler extends WarningHandler<AlterTableResponse> {

    /**
     * Constructor for AlterTableWarningHandler.
     *
     * @param responseObserver The response observer to send warnings to.
     */
    public AlterTableWarningHandler(StreamObserver<AlterTableResponse> responseObserver) {
        super(responseObserver);
    }

    /**
     * Creates an AlterTableResponse containing a warning message.
     *
     * @param message The warning message.
     * @return The AlterTableResponse with the warning.
     */
    @Override
    public AlterTableResponse createWarning(String message) {
        return AlterTableResponse.newBuilder()
                .setWarning(Warning.newBuilder()
                        .setMessage(message)
                        .build())
                .build();
    }
}