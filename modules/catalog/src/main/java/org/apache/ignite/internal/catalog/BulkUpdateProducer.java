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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * Update producer that is used to group updates
 * when executing a batch of catalog commands.
 */
class BulkUpdateProducer implements UpdateProducer {
    private final List<? extends UpdateProducer> commands;

    BulkUpdateProducer(List<? extends UpdateProducer> producers) {
        this.commands = producers;
    }

    @Override
    public List<UpdateEntry> get(UpdateContext updateContext) {
        List<UpdateEntry> bulkUpdateEntries = new ArrayList<>();

        for (UpdateProducer producer : commands) {
            List<UpdateEntry> entries = producer.get(updateContext);

            for (UpdateEntry entry : entries) {
                updateContext.updateCatalog(
                        catalog -> entry.applyUpdate(catalog, CatalogManager.INITIAL_TIMESTAMP)
                );
            }

            bulkUpdateEntries.addAll(entries);
        }

        return bulkUpdateEntries;
    }
}
