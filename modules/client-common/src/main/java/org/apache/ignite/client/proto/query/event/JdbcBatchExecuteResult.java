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
 * JDBC batch execute result.
 */
public class JdbcBatchExecuteResult extends JdbcResponse {
    /** Update counts. */
    private int[] updateCnts;

    /** Batch update error code. */
    private int errCode;

    /** Batch update error message. */
    private String errMsg;

    /**
     * Constructor.
     */
    public JdbcBatchExecuteResult() {
    }

    /**
     * Constructor.
     */
    public JdbcBatchExecuteResult(int status, String err) {
        super(status, err);
    }

    /**
     * @param updateCnts Update counts for batch.
     * @param errCode Error code.
     * @param errMsg Error message.
     */
    public JdbcBatchExecuteResult(int[] updateCnts, int errCode, String errMsg) {
        this.updateCnts = updateCnts;
        this.errCode = errCode;
        this.errMsg = errMsg;
    }

    /**
     * @return Update count for DML queries.
     */
    public int[] updateCounts() {
        return updateCnts;
    }

    /**
     * @return Batch error code.
     */
    public int errorCode() {
        return errCode;
    }

    /**
     * @return Batch error message.
     */
    public String errorMessage() {
        return errMsg;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(ClientMessagePacker packer) throws IOException {
        super.writeBinary(packer);

        if (status() != STATUS_SUCCESS)
            return;

        packer.packInt(errCode);
        packer.packString(errMsg);
        packer.packIntArray(updateCnts);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(ClientMessageUnpacker unpacker) throws IOException {
        super.readBinary(unpacker);

        if (status() != STATUS_SUCCESS)
            return;

        errCode = unpacker.unpackInt();
        errMsg = unpacker.unpackString();
        updateCnts = unpacker.unpackIntArray();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcBatchExecuteResult.class, this);
    }
}
