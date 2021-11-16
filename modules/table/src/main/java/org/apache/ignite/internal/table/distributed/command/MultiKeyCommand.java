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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tx.Timestamp;

/**
 * A multi key transactional command.
 */
public abstract class MultiKeyCommand implements TransactionalCommand, Serializable {
    /** Binary rows. */
    private transient Collection<BinaryRow> rows;

    /** The timestamp. */
    private Timestamp timestamp;

    /*
     * Row bytes.
     * It is a temporary solution, before network have not implement correct serialization BinaryRow.
     * TODO: Remove the field after (IGNITE-14793).
     */
    private byte[] rowsBytes;

    /**
     * @param rows Rows.
     * @param ts The timestamp.
     */
    public MultiKeyCommand(Collection<BinaryRow> rows, Timestamp ts) {
        assert rows != null && !rows.isEmpty();
        this.rows = rows;
        this.timestamp = ts;

        rowsBytes = CommandUtils.rowsToBytes(rows);
    }

    /**
     * Gets a collection of binary rows.
     *
     * @return Binary rows.
     */
    public Collection<BinaryRow> getRows() {
        if (rows == null && rowsBytes != null) {
            rows = new ArrayList<>();

            CommandUtils.readRows(rowsBytes, rows::add);
        }

        return rows;
    }

    /**
     * @return The timestamp.
     */
    @Override
    public Timestamp getTimestamp() {
        return timestamp;
    }
}
