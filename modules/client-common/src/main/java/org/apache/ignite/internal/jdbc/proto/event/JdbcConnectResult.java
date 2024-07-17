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

package org.apache.ignite.internal.jdbc.proto.event;

import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * Result of CONNECT command.
 */
public class JdbcConnectResult extends Response {
    private long connectionId;

    /**
     * Default constructor is used for deserialization.
     */
    public JdbcConnectResult() {
    }

    /**
     * Constructor.
     *
     * @param status Status code.
     * @param err Error message.
     */
    public JdbcConnectResult(int status, String err) {
        super(status, err);
    }

    /**
     * Constructor.
     *
     * @param connectionId An identifier of the connection on a server.
     */
    public JdbcConnectResult(long connectionId) {
        this.connectionId = connectionId;
    }

    /** Returns an identifier of the connection. */
    public long connectionId() {
        return connectionId;
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (!success()) {
            return;
        }

        packer.packLong(connectionId);
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (!success()) {
            return;
        }

        connectionId = unpacker.unpackLong();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(JdbcConnectResult.class, this);
    }
}
