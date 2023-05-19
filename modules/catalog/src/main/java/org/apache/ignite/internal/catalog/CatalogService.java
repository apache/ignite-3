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

package org.apache.ignite.internal.catalog;

import org.apache.ignite.internal.catalog.descriptors.DistributionZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.IndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.SchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.manager.EventListener;

/**
 * Catalog service provides methods to access schema object's descriptors of exact version and/or last actual version at given timestamp,
 * which is logical point-in-time.
 *
 * <p>Catalog service listens distributed schema update event, stores/restores schema evolution history (schema versions) for time-travelled
 * queries purposes and for lazy data evolution purposes.
 *
 * <p>TBD: events
 */
public interface CatalogService {
    String PUBLIC = "PUBLIC";

    TableDescriptor table(String tableName, long timestamp);

    TableDescriptor table(int tableId, long timestamp);

    IndexDescriptor index(int indexId, long timestamp);

    SchemaDescriptor schema(int version);

    DistributionZoneDescriptor zone(String zoneName, long timestamp);

    DistributionZoneDescriptor zone(int zoneId, long timestamp);

    SchemaDescriptor activeSchema(long timestamp);

    void listen(CatalogEvent evt, EventListener<CatalogEventParameters> closure);
}
