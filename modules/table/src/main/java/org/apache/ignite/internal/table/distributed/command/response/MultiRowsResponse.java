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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.distributed.command.CommandUtils;
import org.apache.ignite.internal.table.distributed.command.DeleteAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactAllCommand;
import org.apache.ignite.internal.table.distributed.command.GetAllCommand;
import org.apache.ignite.internal.table.distributed.command.InsertAllCommand;

/**
 * It is a response object to return a collection from the batch operation.
 * @see GetAllCommand
 * @see DeleteAllCommand
 * @see InsertAllCommand
 * @see DeleteExactAllCommand
 */
public class MultiRowsResponse implements Serializable {
    /** Row. */
    private transient Collection<BinaryRow> rows;

    /*
     * Row bytes.
     * It is a temporary solution, before network have not implement correct serialization BinaryRow.
     * TODO: Remove the field after (IGNITE-14793).
     */
    private byte[] rowsBytes;

    /**
     * @param rows Rows.
     */
    public MultiRowsResponse(Collection<BinaryRow> rows) {
        this.rows = rows;

        CommandUtils.rowsToBytes(rows, bytes -> rowsBytes = bytes);
    }

    /**
     * @return Data row.
     */
    public Collection<BinaryRow> getValues() {
        if (rows == null && rowsBytes != null) {
            rows = new HashSet<>();

            CommandUtils.readRows(rowsBytes, rows::add);
        }

        return rows;
    }
}
