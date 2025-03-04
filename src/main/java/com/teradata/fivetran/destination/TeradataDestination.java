package com.teradata.fivetran.destination;

import io.grpc.*;

import java.io.IOException;

/**
 * Example Plugin Connector (gRPC server)
 * In production, it will be stored as a container image.
 */
public class TeradataDestination {

    /**
     * Main method to start the gRPC server.
     *
     * @param args Command line arguments.
     * @throws InterruptedException If the server is interrupted.
     * @throws IOException If an I/O error occurs.
     */
    public static void main(String[] args) throws InterruptedException, IOException {
        // Create and start the gRPC server on port 50052
        Server server = ServerBuilder
                .forPort(50052)
                .addService(new TeradataDestinationServiceImpl())
                .build();

        server.start();
        System.out.println("Destination gRPC server started");

        // Wait for the server to terminate
        server.awaitTermination();
    }
}