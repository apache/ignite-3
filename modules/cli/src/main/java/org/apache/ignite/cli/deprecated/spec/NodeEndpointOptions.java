/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.deprecated.spec;

import org.apache.ignite.cli.deprecated.IgniteCliException;
import picocli.CommandLine;

/**
 * Prepared picocli mixin for generic node hostname option.
 */
class NodeEndpointOptions {
    /**
     * Custom node REST endpoint address.
     */
    @CommandLine.Option(
            names = "--node-endpoint",
            description = "Ignite server node's REST API address and port number",
            paramLabel = "host:port"
    )
    private String endpoint;

    /**
     * Returns REST endpoint port.
     *
     * @return REST endpoint port.
     */
    int port() {
        if (endpoint == null) {
            return 10300;
        }

        var hostPort = parse();

        try {
            return Integer.parseInt(hostPort[1]);
        } catch (NumberFormatException ex) {
            throw new IgniteCliException("Can't parse port from " + hostPort[1] + " value");
        }
    }

    /**
     * Returns REST endpoint host.
     *
     * @return REST endpoint host.
     */
    String host() {
        return endpoint != null ? parse()[0] : "localhost";
    }

    /**
     * Parses REST endpoint host and port from string.
     *
     * @return 2-elements array [host, port].
     */
    private String[] parse() {
        var hostPort = endpoint.split(":");

        if (hostPort.length != 2) {
            throw new IgniteCliException("Incorrect host:port pair provided: '" + endpoint + "' "
                    + "(example of valid value 'localhost:10300')");
        }

        return hostPort;
    }
}
