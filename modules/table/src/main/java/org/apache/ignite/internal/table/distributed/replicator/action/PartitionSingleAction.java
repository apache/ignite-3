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

package org.apache.ignite.internal.table.distributed.replicator.action;

import java.util.UUID;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.table.distributed.replicator.action.container.SingleEntryContainer;

/**
 * The class is used for passing a single entry replication action.
 */
public class PartitionSingleAction extends PartitionAction {
    /**
     * Entry container.
     */
    private final SingleEntryContainer container;

    /**
     * The constructor.
     *
     * @param type       Action type.
     * @param txId       Transaction id.
     * @param timestamp  Transaction timestamp.
     * @param indexToUse Index id.
     * @param container  Entry container.
     */
    public PartitionSingleAction(ActionType type, UUID txId, HybridTimestamp timestamp, UUID indexToUse, SingleEntryContainer container) {
        super(type, txId, timestamp, indexToUse);

        this.container = container;
    }

    /**
     * Gets an entry container.
     *
     * @return Entry container.
     */
    public SingleEntryContainer entryContainer() {
        return container;
    }
}
