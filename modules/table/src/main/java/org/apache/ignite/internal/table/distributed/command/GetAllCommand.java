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
import org.apache.ignite.raft.client.ReadCommand;
import org.jetbrains.annotations.NotNull;

/**
 * This is a command for the batch get operation.
 */
public class GetAllCommand implements MultiKeyCommand, ReadCommand {
    /** Binary key rows. */
    private transient Collection<BinaryRow> keyRows;

    /** The timestamp. */
    private final Timestamp timestamp;

    /*
     * Row bytes.
     * It is a temporary solution, before network have not implement correct serialization BinaryRow.
     * TODO: Remove the field after (IGNITE-14793).
     */
    private byte[] keyRowsBytes;

    /**
     * Creates a new instance of GetAllCommand with the given keys to be got.
     * The {@code keyRows} should not be {@code null} or empty.
     *
     * @param keyRows Binary key rows.
     * @param timestamp The timestamp.
     */
    public GetAllCommand(@NotNull Collection<BinaryRow> keyRows, Timestamp timestamp) {
        assert keyRows != null && !keyRows.isEmpty();

        this.keyRows = keyRows;
        this.timestamp = timestamp;

        CommandUtils.rowsToBytes(keyRows, bytes -> keyRowsBytes = bytes);
    }

    /**
     * Gets a set of binary key rows to be got.
     *
     * @return Binary keys.
     */
    @Override public Collection<BinaryRow> getRows() {
        if (keyRows == null && keyRowsBytes != null) {
            keyRows = new ArrayList<>();

            CommandUtils.readRows(keyRowsBytes, keyRows::add);
        }

        return keyRows;
    }

    /**
     * @return The timestamp.
     */
    @Override public Timestamp getTimestamp() {
        return timestamp;
    }

    /** {@inheritDoc} */
    @Override public boolean read() {
        return true;
    }
}
