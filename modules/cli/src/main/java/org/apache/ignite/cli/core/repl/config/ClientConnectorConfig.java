package org.apache.ignite.cli.core.repl.config;

import org.apache.ignite.client.IgniteClientConfiguration;

/**
 * DTO class for client connector config.
 */
public class ClientConnectorConfig {
    /**
     * Ignite client port.
     */
    public int port = IgniteClientConfiguration.DFLT_PORT;
}
