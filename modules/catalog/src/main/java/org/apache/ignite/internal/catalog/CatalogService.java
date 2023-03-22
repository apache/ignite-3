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

import java.util.Collection;
import org.apache.ignite.internal.catalog.descriptors.IndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.SchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;

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

    //TODO: IGNITE-19082 Drop this stuff when all versioned schema stuff will be moved to Catalog.
    @Deprecated(forRemoval = true)
    String IGNITE_USE_CATALOG_PROPERTY = "IGNITE_USE_CATALOG";

    @Deprecated(forRemoval = true)
    static boolean useCatalogService() {
        return Boolean.getBoolean(IGNITE_USE_CATALOG_PROPERTY);
    }

    TableDescriptor table(String tableName, long timestamp);

    TableDescriptor table(int tableId, long timestamp);

    IndexDescriptor index(int indexId, long timestamp);

    Collection<IndexDescriptor> tableIndexes(int tableId, long timestamp);

    SchemaDescriptor schema(int version);

    SchemaDescriptor activeSchema(long timestamp);
}
