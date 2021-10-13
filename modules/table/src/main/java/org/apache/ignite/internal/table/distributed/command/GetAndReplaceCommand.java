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

package org.apache.ignite.internal.table.distributed.command;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;

/**
 * This is a command to get a value before replace it.
 */
public class GetAndReplaceCommand implements SingleKeyCommand, WriteCommand {
    /** Binary row. */
    private transient BinaryRow row;

    /*
     * Row bytes.
     * It is a temporary solution, before network have not implement correct serialization BinaryRow.
     * TODO: Remove the field after (IGNITE-14793).
     */
    private byte[] rowBytes;

    /** The timestamp. */
    private final Timestamp timestamp;

    /**
     * Creates a new instance of GetAndReplaceCommand with the given row to be got and replaced.
     * The {@code row} should not be {@code null}.
     *
     * @param row Binary row.
     * @param ts The timestamp.
     */
    public GetAndReplaceCommand(@NotNull BinaryRow row, Timestamp ts) {
        assert row != null;

        this.row = row;
        this.timestamp = ts;

        CommandUtils.rowToBytes(row, bytes -> rowBytes = bytes);
    }

    /**
     * Gets a binary row to be got and replaced.
     *
     * @return Binary row.
     */
    @Override public BinaryRow getRow() {
        if (row == null)
            row = new ByteBufferRow(rowBytes);

        return row;
    }

    /**
     * @return The timestamp.
     */
    @Override public Timestamp getTimestamp() {
        return timestamp;
    }

    /** {@inheritDoc} */
    @Override public boolean read() {
        return false;
    }
}
