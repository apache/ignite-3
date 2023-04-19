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

package org.apache.ignite.internal.index.event;

import java.util.UUID;
import org.apache.ignite.internal.index.IndexDescriptor;
import org.apache.ignite.internal.manager.EventParameters;
import org.jetbrains.annotations.Nullable;

/**
 * Index event parameters. There are properties which associate with a particular index.
 */
public class IndexEventParameters extends EventParameters {
    /** Table identifier. */
    private final UUID tableId;

    /** Index identifier. */
    private final UUID indexId;

    /** Index instance. */
    private final @Nullable IndexDescriptor indexDescriptor;

    /**
     * Constructor.
     *
     * @param revision Causality token.
     * @param tableId Table identifier.
     * @param indexId Index identifier.
     */
    public IndexEventParameters(long revision, UUID tableId, UUID indexId) {
        this(revision, tableId, indexId, null);
    }

    /**
     * Constructor.
     *
     * @param revision Causality token.
     * @param tableId Table identifier.
     * @param indexId Index identifier.
     * @param indexDescriptor Index descriptor.
     */
    public IndexEventParameters(long revision, UUID tableId, UUID indexId, @Nullable IndexDescriptor indexDescriptor) {
        super(revision);

        this.tableId = tableId;
        this.indexId = indexId;
        this.indexDescriptor = indexDescriptor;
    }

    /**
     * Returns an identifier of the table this event relates to.
     *
     * @return An id of the table.
     */
    public UUID tableId() {
        return tableId;
    }

    /**
     * Returns an identifier of the index this event relates to.
     *
     * @return An id of the index.
     */
    public UUID indexId() {
        return indexId;
    }

    /**
     * Returns an index instance this event relates to.
     *
     * @return An index.
     */
    public @Nullable IndexDescriptor indexDescriptor() {
        return indexDescriptor;
    }
}
