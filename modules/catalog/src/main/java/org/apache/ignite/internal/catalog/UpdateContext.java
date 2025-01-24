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

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * Context contains the catalog to be updated.
 * It is used by {@link BulkUpdateProducer} when executing a batch of catalog commands
 *
 * @see BulkUpdateProducer
 */
public class UpdateContext {
    /** Catalog descriptor on the basis of which to generate the list of updates. */
    private Catalog catalog;

    /** Identifiers of created tables. */
    private @Nullable Set<Integer> tableIds;

    /** Constructor. */
    public UpdateContext(Catalog catalog) {
        this.catalog = catalog;
    }

    /** Returns catalog descriptor. */
    public Catalog catalog() {
        return catalog;
    }

    /** Registers the table being created in the context. */
    public void registerTableCreation(int tableId) {
        if (tableIds == null) {
            tableIds = new HashSet<>();
        }

        tableIds.add(tableId);
    }

    /** Returns whether the command to create a table with the specified identifier was processed. */
    public boolean containsTableCreation(int tableId) {
        return tableIds != null && tableIds.contains(tableId);
    }

    /** Applies specified action to the catalog. */
    public void updateCatalog(Function<Catalog, Catalog> updater) {
        catalog = updater.apply(catalog);
    }
}
