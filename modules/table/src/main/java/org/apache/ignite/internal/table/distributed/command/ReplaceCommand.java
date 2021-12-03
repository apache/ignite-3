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
 * The command replaces an old entry to a new one.
 */
public class ReplaceCommand extends SingleKeyCommand implements WriteCommand {
    /** Replaced binary row. */
    private transient BinaryRow oldRow;

    /**
     * Old row bytes.
     * TODO: Remove the field after (IGNITE-14793).
     */
    private byte[] oldRowBytes;

    /**
     * Creates a new instance of ReplaceCommand with the given two rows to be replaced each other.
     * Both rows should not be {@code null}.
     *
     * @param oldRow        Old Binary row.
     * @param row           Binary row.
     * @param timestamp     The timestamp.
     *
     * @see TransactionalCommand
     */
    public ReplaceCommand(@NotNull BinaryRow oldRow, @NotNull BinaryRow row, @NotNull Timestamp timestamp) {
        super(row, timestamp);
        assert oldRow != null;

        oldRowBytes = CommandUtils.rowToBytes(oldRow);
    }

    /**
     * Gets a binary row which should be before replace.
     *
     * @return Binary row.
     */
    @NotNull
    public BinaryRow getOldRow() {
        if (oldRow == null) {
            oldRow = new ByteBufferRow(oldRowBytes);
        }

        return oldRow;
    }
}
