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

package org.apache.ignite.internal.catalog.events;

import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;

/**
 * Event parameters for the 'index has moved to the {@link CatalogIndexStatus#STOPPING} that contains an id of the dropped index.
 *
 * @see CatalogEvent#INDEX_STOPPING
 */
public class StoppingIndexEventParameters extends IndexEventParameters {
    private final int tableId;

    /**
     * Constructor.
     *
     * @param causalityToken Causality token.
     * @param catalogVersion Catalog version.
     * @param indexId An id of dropped index.
     * @param tableId Table ID for which the index was removed.
     */
    public StoppingIndexEventParameters(long causalityToken, int catalogVersion, int indexId, int tableId) {
        super(causalityToken, catalogVersion, indexId);

        this.tableId = tableId;
    }

    /** Returns table ID for which the index was removed. */
    public int tableId() {
        return tableId;
    }
}
