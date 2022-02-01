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

package org.apache.ignite.internal.idx.event;

import org.apache.ignite.internal.idx.InternalSortedIndex;
import org.apache.ignite.internal.manager.EventParameters;
import org.jetbrains.annotations.Nullable;

/**
 * Table event parameters. There are properties which associate with a concrete table.
 */
public class IndexEventParameters implements EventParameters {
    /** Index name. */
    private final String idxName;

    /** Indexed table name. */
    private final String tblName;

    /** Table instance. */
    private final InternalSortedIndex idx;

    /**
     * Constructor.
     *
     * @param tblName Table name.
     * @param idx Index instance.
     */
    public IndexEventParameters(String tblName, InternalSortedIndex idx) {
        this(idx.name(), tblName, idx);
    }

    /**
     * Constructor.
     *
     * @param idxName Index name.
     * @param tblName Table name.
     */
    public IndexEventParameters(String idxName, String tblName) {
        this(idxName, tblName, null);
    }

    /**
     * Constructor.
     *
     * @param idxName Index name.
     * @param tblName Table name.
     * @param idx     Index.
     */
    private IndexEventParameters(String idxName, String tblName, @Nullable InternalSortedIndex idx) {
        this.idxName = idxName;
        this.tblName = tblName;
        this.idx = idx;
    }

    public String indexName() {
        return idxName;
    }

    public String tableName() {
        return tblName;
    }

    public InternalSortedIndex index() {
        return idx;
    }
}
