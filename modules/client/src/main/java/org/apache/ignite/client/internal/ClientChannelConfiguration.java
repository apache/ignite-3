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

package org.apache.ignite.client.internal;

import java.net.InetSocketAddress;

/**
 * Configuration required to initialize {@link TcpClientChannel}.
 */
final class ClientChannelConfiguration {
    /** Host. */
    private final InetSocketAddress addr;

    /** Tcp no delay. */
    private final boolean tcpNoDelay;

    /** Timeout. */
    private final int timeout;

    /** Send buffer size. */
    private final int sndBufSize;

    /** Receive buffer size. */
    private final int rcvBufSize;

    /**
     * Constructor.
     */
    @SuppressWarnings("UnnecessaryThis")
    ClientChannelConfiguration(Object cfg, InetSocketAddress addr) {
        // TODO: Get from public API cfg.
        this.tcpNoDelay = true;
        this.timeout = 0;
        this.sndBufSize = 0;
        this.rcvBufSize = 0;
        this.addr = addr;
    }

    /**
     * @return Address.
     */
    public InetSocketAddress getAddress() {
        return addr;
    }

    /**
     * @return Tcp no delay.
     */
    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * @return Timeout.
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * @return Send buffer size.
     */
    public int getSendBufferSize() {
        return sndBufSize;
    }

    /**
     * @return Receive buffer size.
     */
    public int getReceiveBufferSize() {
        return rcvBufSize;
    }
}
