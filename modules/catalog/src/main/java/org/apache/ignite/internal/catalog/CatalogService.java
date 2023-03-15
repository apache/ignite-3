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
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.descriptors.IndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.SchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;
import org.apache.ignite.internal.configuration.DynamicConfigurationChanger;

/**
 * Catalog service provides methods to access schema object's descriptors of exact version and/or last actual version at given timestamp,
 * which is logical point-in-time.
 *
 * <p>Catalog service is responsible for proper configuration updates and storing/restoring schema evolution history (schema versions)
 * for time-travelled queries purposes and lazy data evolution purposes.
 *
 * <p>TBD: schema manipulation methods.
 * TBD: events
 */
public interface CatalogService {
    TableDescriptor table(String tableName, long timestamp);

    TableDescriptor table(int tableId, long timestamp);

    IndexDescriptor index(int indexId, long timestamp);

    Collection<IndexDescriptor> tableIndexes(int tableId, long timestamp);

    SchemaDescriptor schema(int version);

    SchemaDescriptor activeSchema(long timestamp);

    //TODO: IGNITE-18535 enrich with schema manipulation methods.
    CompletableFuture<SchemaDescriptor> updateSchema(DynamicConfigurationChanger changer);
}
