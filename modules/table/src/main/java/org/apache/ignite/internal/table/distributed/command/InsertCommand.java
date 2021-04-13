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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.raft.client.WriteCommand;

public class InsertCommand implements WriteCommand {
    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(GetCommand.class);

    /** Row. */
    private transient BinaryRow row;

    /*
     * Row bytes.
     * It is a temporary solution, before network have not implement correct serialization BinaryRow.
     * TODO: Remove the field after.
     */
    private byte[] rowBytes;

    public InsertCommand(BinaryRow row) {
        this.row = row;

        keyToBytes(row);
    }

    private void keyToBytes(BinaryRow row) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            row.writeTo(baos);

            baos.flush();

            rowBytes = baos.toByteArray();
        }
        catch (IOException e) {
            LOG.error("Could not write row to stream [row=" + row + ']', e);

            rowBytes = null;
        }
    }

    /**
     * Gets a data row.
     *
     * @return Data row.
     */
    public BinaryRow getRow() {
        if (row == null)
            row = new ByteBufferRow(rowBytes);

        return row;
    }
}
