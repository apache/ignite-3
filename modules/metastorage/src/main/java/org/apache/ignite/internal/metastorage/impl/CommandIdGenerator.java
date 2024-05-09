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

package org.apache.ignite.internal.metastorage.impl;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.ignite.internal.metastorage.CommandId;
import org.apache.ignite.internal.metastorage.dsl.MetaStorageMessagesFactory;

/**
 * Generates the command ids.
 */
public class CommandIdGenerator {
    private static final MetaStorageMessagesFactory MSG_FACTORY = new MetaStorageMessagesFactory();

    /** Supplies nodeId for transactionId generation. */
    private final Supplier<String> nodeIdSupplier;

    private volatile UUID nodeId;

    private final AtomicLong counter = new AtomicLong();

    public CommandIdGenerator(Supplier<String> nodeIdSupplier) {
        this.nodeIdSupplier = nodeIdSupplier;
    }

    /**
     * New id.
     *
     * @return New command id.
     */
    public CommandId newId() {
        if (nodeId == null) {
            synchronized (this) {
                if (nodeId == null) {
                    String nodeIdString = nodeIdSupplier.get();
                    nodeId = UUID.fromString(nodeIdString);
                }
            }
        }

        return MSG_FACTORY.commandId()
                .nodeId(nodeId)
                .counter(counter.getAndIncrement())
                .build();
    }
}
