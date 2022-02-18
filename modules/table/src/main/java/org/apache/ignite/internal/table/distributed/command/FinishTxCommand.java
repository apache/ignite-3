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
import java.util.Collections;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.raft.client.WriteCommand;

/** State machine command to finish a transaction. */
public class FinishTxCommand implements WriteCommand {
    /** The timestamp. */
    private final Timestamp timestamp;

    /** Commit or rollback state. */
    private final boolean finish;

    /** Rows that were updated in any way by current transaction. */
    private transient Collection<BinaryRow> writeRows;

    /** Serialized {@link #writeRows}. */
    private final byte[] rowsBytes;

    /**
     * The constructor.
     *
     * @param timestamp The timestamp.
     * @param finish    Commit or rollback state {@code True} to commit.
     * @param rows Rows which were updated by current transaction.
     */
    public FinishTxCommand(Timestamp timestamp, boolean finish, Collection<BinaryRow> rows) {
        this.timestamp = timestamp;
        this.finish = finish;
        this.writeRows = rows;

        rowsBytes = CommandUtils.rowsToBytes(rows);
    }

    /**
     * Returns a timestamp.
     *
     * @return The timestamp.
     */
    public Timestamp timestamp() {
        return timestamp;
    }

    /**
     * Returns commit or rollback state.
     *
     * @return Commit or rollback state.
     */
    public boolean finish() {
        return finish;
    }

    /**
     * Returns collection of rows which were updated by current transaction.
     *
     * @return Collection of rows which were updated by current transaction.
     */
    public Collection<BinaryRow> writeRows() {
        if (writeRows == null && rowsBytes != null) {
            writeRows = new ArrayList<>();

            CommandUtils.readRows(rowsBytes, writeRows::add);
        }

        if (writeRows == null) {
            writeRows = Collections.emptyList();
        }

        return writeRows;
    }
}
