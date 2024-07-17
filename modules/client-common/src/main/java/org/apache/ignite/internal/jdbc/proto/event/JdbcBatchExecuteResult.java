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

import java.util.Objects;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.ArrayUtils;

/**
 * JDBC batch execute result.
 */
public class JdbcBatchExecuteResult extends Response {
    /** Update counts. */
    private int[] updateCnts;

    /** Error code. */
    private int errorCode;

    /**
     * Constructor.
     */
    public JdbcBatchExecuteResult() {
    }

    /**
     * Constructor.
     *
     * @param status Status code.
     * @param errorCode Product error code.
     * @param errorMessage Error message.
     * @param updateCnts Array with success and error markers.
     */
    public JdbcBatchExecuteResult(int status, int errorCode, String errorMessage, int[] updateCnts) {
        super(status, errorMessage);

        this.errorCode = errorCode;
        this.updateCnts = updateCnts;
    }

    /**
     * Constructor.
     *
     * @param status Status code.
     * @param err    Error message.
     */
    public JdbcBatchExecuteResult(int status, String err) {
        super(status, err);
    }

    /**
     * Constructor.
     *
     * @param updateCnts Update counts for batch.
     */
    public JdbcBatchExecuteResult(int[] updateCnts) {
        Objects.requireNonNull(updateCnts);

        this.updateCnts = updateCnts;
    }

    /**
     * Get the update count for DML queries.
     *
     * @return Update count for DML queries.
     */
    public int[] updateCounts() {
        return updateCnts == null ? ArrayUtils.INT_EMPTY_ARRAY : updateCnts;
    }

    /**
     * Get the error code.
     *
     * @return Error code.
     */
    public int getErrorCode() {
        return errorCode;
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        packer.packInt(errorCode);

        packer.packIntArray(updateCnts);
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        errorCode = unpacker.unpackInt();

        if (unpacker.tryUnpackNil()) {
            updateCnts = ArrayUtils.INT_EMPTY_ARRAY;
        } else {
            updateCnts = unpacker.unpackIntArray();
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(JdbcBatchExecuteResult.class, this);
    }
}
