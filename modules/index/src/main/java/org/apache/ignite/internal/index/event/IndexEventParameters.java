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

package org.apache.ignite.internal.index.event;

import java.util.UUID;
import org.apache.ignite.internal.index.Index;
import org.apache.ignite.internal.manager.EventParameters;
import org.jetbrains.annotations.Nullable;

/**
 * Index event parameters. There are properties which associate with a particular index.
 */
public class IndexEventParameters extends EventParameters {
    /** Index identifier. */
    private final UUID indexId;

    /** Index name. */
    private final String indexName;

    /** Index instance. */
    private final @Nullable Index<?> index;

    /**
     * Constructor.
     *
     * @param revision Causality token.
     * @param index An index instance.
     */
    public IndexEventParameters(long revision, Index<?> index) {
        this(revision, index.id(), index.name(), index);
    }

    /**
     * Constructor.
     *
     * @param revision Causality token.
     * @param indexId An index identifier.
     * @param indexName An index name.
     */
    public IndexEventParameters(long revision, UUID indexId, String indexName) {
        this(revision, indexId, indexName, null);
    }

    /**
     * Constructor.
     *
     * @param revision Causality token.
     * @param indexId An index identifier.
     * @param indexName An index name.
     * @param index An index instance.
     */
    protected IndexEventParameters(long revision, UUID indexId, String indexName, @Nullable Index<?> index) {
        super(revision);

        this.indexId = indexId;
        this.indexName = indexName;
        this.index = index;
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
     * Returns a name of the index this event relates to.
     *
     * @return A name of the index.
     */
    public String indexName() {
        return indexName;
    }

    /**
     * Returns an index instance this event relates to.
     *
     * @return An index.
     */
    public @Nullable Index<?> index() {
        return index;
    }
}
