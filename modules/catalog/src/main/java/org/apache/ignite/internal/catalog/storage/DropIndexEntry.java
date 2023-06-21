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

package org.apache.ignite.internal.catalog.storage;

import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.DropIndexEventParameters;
import org.apache.ignite.internal.tostring.S;

/**
 * Describes deletion of an index.
 */
// TODO: IGNITE-19641 добавить идентификатор таблицы
public class DropIndexEntry implements UpdateEntry, CatalogFireEvent {
    private static final long serialVersionUID = -604729846502020728L;

    private final int indexId;

    /**
     * Constructs the object.
     *
     * @param indexId ID of an index to drop.
     */
    public DropIndexEntry(int indexId) {
        this.indexId = indexId;
    }

    /**
     * Returns ID of an index to drop.
     */
    public int indexId() {
        return indexId;
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.INDEX_DROP;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new DropIndexEventParameters(causalityToken, catalogVersion, indexId);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
