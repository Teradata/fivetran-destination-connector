package com.teradata.fivetran.destination;

import io.grpc.*;
import org.apache.commons.cli.*;

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

        // Create Options object
        Options options = new Options();
        options.addOption("p", "port", true, "Port to run the gRPC server");

        // Parse command line arguments
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        int port = 50052; // Default port

        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("port")) {
                port = Integer.parseInt(cmd.getOptionValue("port"));
            }
        } catch (ParseException | NumberFormatException e) {
            System.err.println("Invalid port number, using default port 50052");
        }

        // Create and start the gRPC server on the specified port
        Server server = ServerBuilder
                .forPort(port)
                .addService(new TeradataDestinationServiceImpl())
                .build();

        // Start the server
        server.start();
        System.out.println("Destination gRPC server started on port " + port);

        // Wait for the server to terminate
        server.awaitTermination();
    }
}