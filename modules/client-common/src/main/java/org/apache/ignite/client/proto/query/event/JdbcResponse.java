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

package org.apache.ignite.client.proto.query.event;

import java.io.IOException;
import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * SQL listener response.
 */
public abstract class JdbcResponse implements JdbcClientMessage {
    /** Command succeeded. */
    public static final int STATUS_SUCCESS = 0;

    /** Command failed. */
    public static final int STATUS_FAILED = 1;

    /** Success status. */
    private int status;

    /** Error. */
    private String err;

    /**
     * Constructs successful response.
     */
    protected JdbcResponse() {
        status = STATUS_SUCCESS;
    }

    /**
     * Constructs failed rest response.
     *
     * @param status Response status.
     * @param err Error, {@code null} if success is {@code true}.
     */
    protected JdbcResponse(int status, String err) {
        assert status != STATUS_SUCCESS;

        this.status = status;
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(ClientMessagePacker packer) throws IOException {
        packer.packInt(status);

        if (status != STATUS_SUCCESS)
            packer.packString(err);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(ClientMessageUnpacker unpacker) throws IOException {
        status = unpacker.unpackInt();

        if (status != STATUS_SUCCESS)
            err = unpacker.unpackString();
    }

    /**
     * @return Success status.
     */
    public int status() {
        return status;
    }

    /**
     * @param status New success status.
     */
    public void status(int status) {
        this.status = status;
    }

    /**
     * @return Error.
     */
    public String err() {
        return err;
    }

    /**
     * @param err New error.
     */
    public void err(String err) {
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcResponse.class, this);
    }
}
