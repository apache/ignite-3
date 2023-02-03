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

package org.apache.ignite.internal.sql.engine.message;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.engine.metadata.FragmentDescription;
import org.apache.ignite.network.annotations.Marshallable;
import org.apache.ignite.network.annotations.Transferable;
import org.jetbrains.annotations.Nullable;

/**
 * QueryStartRequest interface.
 */
@Transferable(value = SqlQueryMessageGroup.QUERY_START_REQUEST)
public interface QueryStartRequest extends ExecutionContextAwareMessage {
    /**
     * Get schema name.
     */
    String schema();

    /**
     * Get fragment description.
     */
    @Marshallable
    FragmentDescription fragmentDescription();

    /**
     * Get fragment plan.
     */
    String root();

    /**
     * Get query parameters.
     */
    @Marshallable
    Object[] parameters();

    /**
     * Read only transaction time or null if this is read write transaction.
     */
    @Marshallable
    @Nullable HybridTimestamp txTime();

    /**
     * Transaction id.
     */
    @Marshallable
    UUID txId();
}
