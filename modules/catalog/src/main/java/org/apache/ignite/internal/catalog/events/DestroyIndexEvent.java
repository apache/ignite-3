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

/** {@link CatalogEvent#INDEX_DESTROY} event. */
public class DestroyIndexEvent implements Fireable {
    private final int tableId;
    private final int indexId;
    private final int partitions;

    /**
     * Constructor.
     *
     * @param indexId An id of dropped index.
     * @param tableId An id of table the index belongs to.
     * @param partitions Table partitions.
     */
    public DestroyIndexEvent(int indexId, int tableId, int partitions) {
        this.indexId = indexId;
        this.tableId = tableId;
        this.partitions = partitions;
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.INDEX_DESTROY;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new DestroyIndexEventParameters(causalityToken, catalogVersion, indexId, tableId, partitions);
    }
}
