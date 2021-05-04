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

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.raft.client.ReadCommand;

/**
 * This is a command for the batch get operation.
 */
public class GetAllCommand implements ReadCommand {
    /** Key row. */
    private transient Set<BinaryRow> keyRows;

    /*
     * Row bytes.
     * It is a temporary solution, before network have not implement correct serialization BinaryRow.
     * TODO: Remove the field after.
     */
    private byte[] keyRowsBytes;

    /**
     * @param keyRows Key rows.
     */
    public GetAllCommand(Set<BinaryRow> keyRows) {
        assert keyRows != null && !keyRows.isEmpty();

        this.keyRows = keyRows;

        CommandUtils.rowsToBytes(keyRows, bytes -> keyRowsBytes = bytes);
    }

    /**
     * Gets a list of keys which will used in the command.
     *
     * @return List keys.
     */
    public Set<BinaryRow> getKeyRows() {
        if (keyRows == null && keyRowsBytes != null) {
            keyRows = new HashSet<>();

            CommandUtils.readRows(keyRowsBytes, keyRows::add);
        }

        return keyRows;
    }
}
