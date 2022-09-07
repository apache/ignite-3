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
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.distributed.command.CommandUtils;
import org.apache.ignite.internal.table.distributed.command.DeleteAllCommand;
import org.apache.ignite.internal.table.distributed.command.InsertAndUpdateAllCommand;

/**
 * This class represents a response object that contains a collection {@link BinaryRow} from a batch operation.
 *
 * @see DeleteAllCommand
 * @see InsertAndUpdateAllCommand
 */
public class MultiRowsResponse implements Serializable {
    /** Binary rows. */
    private transient List<BinaryRow> rows;

    /*
     * Row bytes.
     * It is a temporary solution, before network have not implement correct serialization BinaryRow.
     * TODO: Remove the field after (IGNITE-14793).
     */
    private byte[] rowsBytes;

    /**
     * Creates a new instance of MultiRowsResponse with the given collection of binary rows.
     *
     * @param rows Collection of binary rows.
     */
    public MultiRowsResponse(List<BinaryRow> rows) {
        this.rows = rows;

        rowsBytes = CommandUtils.rowsToBytes(rows);
    }

    /**
     * Returns binary rows.
     */
    public List<BinaryRow> getValues() {
        if (rows == null && rowsBytes != null) {
            rows = new ArrayList<>();

            CommandUtils.readRows(rowsBytes, rows::add);
        }

        return rows;
    }
}
