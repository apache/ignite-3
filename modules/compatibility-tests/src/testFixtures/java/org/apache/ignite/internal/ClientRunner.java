package org.apache.ignite.internal;

public class ClientRunner {
    public static void runClient(String version) {
        // 1. Use constructArgFile to resolve dependencies of a given client version.
        // 2. Use Cytodynamics to run the client with the constructed arg file in an isolated classloader.
    }
}
