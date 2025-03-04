package com.teradata.fivetran.destination.warning_util;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class to handle warnings and send them to a response observer.
 *
 * @param <T> The type of response.
 */
public abstract class WarningHandler<T> {
    protected final StreamObserver<T> responseObserver;
    protected static final Logger logger = LoggerFactory.getLogger(WarningHandler.class);

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
        logger.warn(message);
        responseObserver.onNext(createWarning(message));
    }

    /**
     * Handles a warning message and throwable by logging them and sending the message to the response observer.
     *
     * @param message The warning message.
     * @param t       The throwable associated with the warning.
     */
    public void handle(String message, Throwable t) {
        logger.warn(message, t);
        responseObserver.onNext(createWarning(String.format("%s: %s", message, t.getMessage())));
    }

    /**
     * Creates a warning response.
     *
     * @param message The warning message.
     * @return The warning response.
     */
    public abstract T createWarning(String message);
}