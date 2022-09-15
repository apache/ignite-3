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

package org.apache.ignite.internal.table.distributed.command;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.schema.BinaryRow;
import org.jetbrains.annotations.NotNull;

/**
 * A multi key transactional command.
 */
public abstract class MultiKeyCommand implements TransactionalCommand, Serializable {
    /** Binary rows. */
    private transient Collection<BinaryRow> rows;

    /** Transaction id. */
    private @NotNull UUID txId;

    /*
     * Row bytes.
     * It is a temporary solution, before network have not implement correct serialization BinaryRow.
     * TODO: Remove the field after (IGNITE-14793).
     */
    private byte[] rowsBytes;

    /**
     * The constructor.
     *
     * @param rows Rows.
     * @param txId Transaction id.
     */
    public MultiKeyCommand(@NotNull Collection<BinaryRow> rows, @NotNull UUID txId) {
        assert rows != null && !rows.isEmpty();
        this.rows = rows;
        this.txId = txId;

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
     * Returns a transaction id.
     *
     * @return The transaction id.
     */
    @NotNull
    @Override
    public UUID getTxId() {
        return txId;
    }
}
