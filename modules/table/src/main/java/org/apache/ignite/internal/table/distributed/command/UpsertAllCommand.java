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

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.raft.client.WriteCommand;

/**
 * The command puts a batch rows.
 */
public class UpsertAllCommand implements MultiKeyCommand, WriteCommand {
    /** Binary rows. */
    private transient Collection<BinaryRow> rows;

    /*
     * Row bytes.
     * It is a temporary solution, before network have not implement correct serialization BinaryRow.
     * TODO: Remove the field after (IGNITE-14793).
     */
    private byte[] rowsBytes;

    /** The timestamp. */
    private final Timestamp timestamp;

    /**
     * Creates a new instance of UpsertAllCommand with the given rows to be upserted.
     * The {@code rows} should not be {@code null} or empty.
     *
     * @param rows Binary rows.
     */
    public UpsertAllCommand(Collection<BinaryRow> rows, Timestamp ts) {
        assert rows != null && !rows.isEmpty();

        this.rows = rows;
        this.timestamp = ts;

        CommandUtils.rowsToBytes(rows, bytes -> rowsBytes = bytes);
    }

    /**
     * Gets a set of binary rows to be upserted.
     *
     * @return Binary rows.
     */
    @Override public Collection<BinaryRow> getRows() {
        if (rows == null && rowsBytes != null) {
            rows = new ArrayList<>();

            CommandUtils.readRows(rowsBytes, rows::add);
        }

        return rows;
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
