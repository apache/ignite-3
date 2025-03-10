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

import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.BUILDING;

import org.apache.ignite.internal.catalog.commands.StartBuildingIndexCommand;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.StartBuildingIndexEventParameters;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntryType;

/** Entry for {@link StartBuildingIndexCommand}. */
public class StartBuildingIndexEntry extends AbstractChangeIndexStatusEntry implements Fireable {
    /** Constructor. */
    public StartBuildingIndexEntry(int indexId) {
        super(indexId, BUILDING);
    }

    @Override
    public CatalogEvent eventType() {
        return CatalogEvent.INDEX_BUILDING;
    }

    @Override
    public CatalogEventParameters createEventParameters(long causalityToken, int catalogVersion) {
        return new StartBuildingIndexEventParameters(causalityToken, catalogVersion, indexId);
    }

    @Override
    public int typeId() {
        return MarshallableEntryType.START_BUILDING_INDEX.id();
    }
}
