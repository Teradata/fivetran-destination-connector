package com.teradata.fivetran.destination.warning_util;

import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringEscapeUtils;

/**
 * Abstract class to handle warnings and send them to a response observer.
 *
 * @param <T> The type of response.
 */
public abstract class WarningHandler<T> {
    protected final StreamObserver<T> responseObserver;

    /**
     * Constructor for WarningHandler.
     *
     * @param responseObserver The response observer to send warnings to.
     */
    public WarningHandler(StreamObserver<T> responseObserver) {
        this.responseObserver = responseObserver;
    }

    /**
     * Handles a warning message by logging it and sending it to the response observer.
     *
     * @param message The warning message.
     */
    public void handle(String message) {
        logMessage("WARNING", message);
        responseObserver.onNext(createWarning(message));
    }

    /**
     * Handles a warning message and throwable by logging them and sending the message to the response observer.
     *
     * @param message The warning message.
     * @param t       The throwable associated with the warning.
     */
    public void handle(String message, Throwable t) {
        logMessage("WARNING", message);
        responseObserver.onNext(createWarning(String.format("%s: %s", message, t.getMessage())));
    }

    /**
     * Creates a warning response.
     *
     * @param message The warning message.
     * @return The warning response.
     */
    public abstract T createWarning(String message);

    private void logMessage(String level, String message) {
        message = StringEscapeUtils.escapeJava(message);
        System.out.println(String.format("{\"level\":\"%s\", \"message\": \"%s\", \"message-origin\": \"sdk_destination\"}", level, message));
    }
}