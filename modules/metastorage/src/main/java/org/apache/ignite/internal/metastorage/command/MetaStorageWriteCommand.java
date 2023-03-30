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

package org.apache.ignite.internal.metastorage.command;

import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.network.annotations.WithSetter;

/** Base meta storage write command. */
public interface MetaStorageWriteCommand extends WriteCommand {
    /**
     * Returns time on the initiator node.
     */
    HybridTimestampMessage initiatorTime();

    /**
     * This is a dirty hack. This time is set by the leader node to disseminate new safe time across
     * followers and learners.
     */
    @WithSetter
    HybridTimestampMessage safeTime();

    /**
     * Setter for the safeTime field.
     */
    default void safeTime(HybridTimestampMessage safeTime) {
        // No-op.
    }
}
