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

import org.apache.ignite.internal.catalog.storage.Fireable;

/** {@link CatalogEvent#INDEX_REMOVED} event. */
public class DestroyIndexEvent implements Fireable {
    private final int indexId;
    private final int tableId;

    /**
     * Constructor.
     *
     * @param indexId An id of dropping index.
     * @param tableId Table id for which the index was removed.
     */
    public DestroyIndexEvent(int indexId, int tableId) {
        this.indexId = indexId;
        this.tableId = tableId;
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.INDEX_REMOVED;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new RemoveIndexEventParameters(causalityToken, catalogVersion, indexId, tableId);
    }
}
