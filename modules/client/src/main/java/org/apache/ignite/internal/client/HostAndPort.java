/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client;

import java.io.Serializable;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import org.apache.ignite.lang.IgniteException;

/**
 * Represents address along with port.
 */
public class HostAndPort implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Host. */
    private final String host;

    /** Port from. */
    private final int port;

    /**
     * Parse string into host and port pair.
     *
     * @param addrStr      String.
     * @param dfltPort Default port.
     * @param errMsgPrefix Error message prefix.
     * @return Result.
     * @throws IgniteException If failed.
     */
    public static HostAndPort parse(String addrStr, int dfltPort, String errMsgPrefix)
            throws IgniteException {
        String host;
        String portStr;
        int port;

        if (addrStr == null || addrStr.isEmpty()) {
            throw createParseError(addrStr, errMsgPrefix, "Address is empty");
        }

        if (addrStr.charAt(0) == '[') { // IPv6 with port(s)
            int hostEndIdx = addrStr.indexOf(']');

            if (hostEndIdx == -1) {
                throw createParseError(addrStr, errMsgPrefix, "Failed to parse IPv6 address, missing ']'");
            }

            host = addrStr.substring(1, hostEndIdx);

            if (hostEndIdx == addrStr.length() - 1) { // no port specified, using default
                port = dfltPort;
            } else { // port specified
                portStr = addrStr.substring(hostEndIdx + 2);
                port = parsePort(portStr, addrStr, errMsgPrefix);
            }
        } else { // IPv4 || IPv6 without port || empty host
            final int colIdx = addrStr.lastIndexOf(':');

            if (colIdx > 0) {
                if (addrStr.lastIndexOf(':', colIdx - 1) != -1) { // IPv6 without [] and port
                    try {
                        Inet6Address.getByName(addrStr);
                        host = addrStr;
                        port = dfltPort;
                    } catch (UnknownHostException e) {
                        throw createParseError(addrStr, errMsgPrefix, "IPv6 is incorrect", e);
                    }
                } else {
                    host = addrStr.substring(0, colIdx);
                    portStr = addrStr.substring(colIdx + 1);
                    port = parsePort(portStr, addrStr, errMsgPrefix);
                }
            } else if (colIdx == 0) {
                throw createParseError(addrStr, errMsgPrefix, "Host name is empty");
            } else { // Port is not specified, use defaults.
                host = addrStr;

                port = dfltPort;
            }
        }

        return new HostAndPort(host, port);
    }

    /**
     * Parse port.
     *
     * @param portStr      Port string.
     * @param addrStr      Address string.
     * @param errMsgPrefix Error message prefix.
     * @return Parsed port.
     * @throws IgniteException If failed.
     */
    private static int parsePort(String portStr, String addrStr, String errMsgPrefix) throws IgniteException {
        try {
            int port = Integer.parseInt(portStr);

            if (port <= 0 || port > 65535) {
                throw createParseError(addrStr, errMsgPrefix, "invalid port " + portStr);
            }

            return port;
        } catch (NumberFormatException ignored) {
            throw createParseError(addrStr, errMsgPrefix, "invalid port " + portStr);
        }
    }

    /**
     * Create parse error.
     *
     * @param addrStr      Address string.
     * @param errMsgPrefix Error message prefix.
     * @param errMsg       Error message.
     * @return Exception.
     */
    private static IgniteException createParseError(String addrStr, String errMsgPrefix, String errMsg) {
        return new IgniteException(errMsgPrefix + " (" + errMsg + "): " + addrStr);
    }

    /**
     * Create parse error with cause - nested exception.
     *
     * @param addrStr      Address string.
     * @param errMsgPrefix Error message prefix.
     * @param errMsg       Error message.
     * @param cause        Cause exception.
     * @return Exception.
     */
    private static IgniteException createParseError(String addrStr, String errMsgPrefix, String errMsg, Throwable cause) {
        return new IgniteException(errMsgPrefix + " (" + errMsg + "): " + addrStr, cause);
    }

    /**
     * Constructor.
     *
     * @param host Host.
     * @param port Port.
     */
    public HostAndPort(String host, int port) {
        assert host != null && !host.isEmpty();

        this.host = host;
        this.port = port;
    }

    /**
     * Returns host.
     *
     * @return Host.
     */
    public String host() {
        return host;
    }

    /**
     * Returns port from.
     *
     * @return Port from.
     */
    public int port() {
        return port;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (o instanceof HostAndPort) {
            HostAndPort other = (HostAndPort) o;

            return host.equals(other.host) && port == other.port;
        } else {
            return false;
        }
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int res = host.hashCode();

        res = 31 * res + port;

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return host + ":" + port;
    }
}
