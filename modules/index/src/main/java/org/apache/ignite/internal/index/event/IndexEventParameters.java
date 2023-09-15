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

import org.apache.ignite.internal.manager.EventParameters;

/** Index event parameters. There are properties which associate with a particular index. */
public class IndexEventParameters extends EventParameters {
    private final int tableId;

    private final int indexId;

    private final int catalogVersion;

    /**
     * Constructor.
     *
     * @param revision Causality token.
     * @param catalogVersion Catalog version.
     * @param tableId Table ID.
     * @param indexId Index ID.
     */
    public IndexEventParameters(long revision, int catalogVersion, int tableId, int indexId) {
        super(revision);

        this.catalogVersion = catalogVersion;
        this.tableId = tableId;
        this.indexId = indexId;
    }

    /** Returns table ID this event relates to. */
    public int tableId() {
        return tableId;
    }

    /** Returns index ID this event relates to. */
    public int indexId() {
        return indexId;
    }

    /** Returns catalog version this event relates to. */
    public int catalogVersion() {
        return catalogVersion;
    }
}
