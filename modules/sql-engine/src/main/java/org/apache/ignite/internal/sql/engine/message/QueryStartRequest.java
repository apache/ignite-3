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

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.annotations.Marshallable;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.replicator.message.TimestampAware;
import org.apache.ignite.internal.sql.engine.exec.TxAttributes;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;

/**
 * QueryStartRequest interface.
 */
@Transferable(value = SqlQueryMessageGroup.QUERY_START_REQUEST)
public interface QueryStartRequest extends TimestampAware, ExecutionContextAwareMessage {
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
     * Transaction id.
     */
    @Marshallable
    TxAttributes txAttributes();

    /**
     * Returns the version of the catalog the enclosed fragment was prepared on. Must be used to resolve schema objects
     * (like tables or system views) during deserialization.
     */
    int catalogVersion();

    /**
     * Session time-zone ID.
     */
    String timeZoneId();

    /** Time of the operation. */
    HybridTimestamp operationTime();
}
