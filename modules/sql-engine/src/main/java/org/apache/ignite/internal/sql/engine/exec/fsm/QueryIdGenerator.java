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

package org.apache.ignite.internal.sql.engine.exec.fsm;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates query IDs.
 */
public class QueryIdGenerator {
    private final AtomicLong counter = new AtomicLong();

    private final int nodeId;

    /**
     * Constructs the generator.
     *
     * @param nodeId Unique identifier of a node within the cluster.
     */
    public QueryIdGenerator(int nodeId) {
        this.nodeId = nodeId;
    }

    /** Returns next id. */
    public UUID next() {
        return new UUID(nodeId, counter.getAndIncrement());
    }
}
