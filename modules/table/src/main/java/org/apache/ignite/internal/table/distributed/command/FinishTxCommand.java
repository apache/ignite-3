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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.raft.client.WriteCommand;

/** State machine command to finish a transaction. */
public class FinishTxCommand implements WriteCommand {
    /** The timestamp. */
    private final UUID id;

    /** Commit or rollback state. */
    private final boolean finish;

    public Map<IgniteUuid, List<byte[]>> lockedKeys;

    /**
     * The constructor.
     *
     * @param id The id.
     * @param finish    Commit or rollback state {@code True} to commit.
     */
    public FinishTxCommand(UUID id, boolean finish, Map<IgniteUuid, List<byte[]>> lockedKeys) {
        this.id = id;
        this.finish = finish;
        this.lockedKeys = lockedKeys;
    }

    /**
     * Returns a timestamp.
     *
     * @return The timestamp.
     */
    public UUID id() {
        return id;
    }

    /**
     * Returns commit or rollback state.
     *
     * @return Commit or rollback state.
     */
    public boolean finish() {
        return finish;
    }
}
