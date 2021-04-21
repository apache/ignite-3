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

package org.apache.ignite.internal.table.distributed.command.response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.function.Consumer;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.table.distributed.command.GetCommand;
import org.apache.ignite.lang.IgniteLogger;

/**
 * It is a response object for handling a table get command.
 * {@see org.apache.ignite.internal.table.distributed.command.GetCommand}
 */
public class KVGetResponse implements Serializable {
    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(GetCommand.class);

    /** Row. */
    private transient BinaryRow row;

    /*
     * Row bytes.
     * It is a temporary solution, before network have not implement correct serialization BinaryRow.
     * TODO: Remove the field after.
     */
    private byte[] rowBytes;

    public KVGetResponse(BinaryRow row) {
        this.row = row;

        rowToBytes(row, bytes -> rowBytes = bytes);
    }

    /**
     * Writes a row to byte array.
     *
     * @param row Row.
     * @param consumer Byte array consumer.
     */
    private void rowToBytes(BinaryRow row, Consumer<byte[]> consumer) {
        if (row == null) {
            consumer.accept(null);

            return;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            row.writeTo(baos);

            baos.flush();

            consumer.accept(baos.toByteArray());
        }
        catch (IOException e) {
            LOG.error("Could not write row to stream [row=" + row + ']', e);

            consumer.accept(null);
        }
    }

    /**
     * @return Data row.
     */
    public BinaryRow getValue() {
        if (row == null && rowBytes != null)
            row = new ByteBufferRow(rowBytes);

        return row;
    }
}
