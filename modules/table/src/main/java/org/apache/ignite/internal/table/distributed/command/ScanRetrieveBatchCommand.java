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

import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;

public class ScanRetrieveBatchCommand implements WriteCommand {
    /** Amount of items to retrieve. */
    private final long itemsToRetrieveCnt;

    /** Id of scan that is associated with the current command. */
    @NotNull private final IgniteUuid scanId;

    /**
     * @param itemsToRetrieveCnt Amount of items to retrieve.
     * @param scanId Id of scan that is associated with the current command.
     */
    public ScanRetrieveBatchCommand(
        long itemsToRetrieveCnt,
        @NotNull IgniteUuid scanId
    ) {
        this.itemsToRetrieveCnt = itemsToRetrieveCnt;
        this.scanId = scanId;
    }

    /**
     * @return Amount of items to retrieve.
     */
    public long itemsToRetrieveCount() {
        return itemsToRetrieveCnt;
    }

    /**
     * @return Id of scan that is associated with the current command.
     */
    @NotNull public IgniteUuid scanId() {
        return scanId;
    }
}
