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

package org.apache.ignite.internal.catalog.commands;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.indexOrThrow;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.BUILDING;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.REGISTERED;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.ChangeIndexStatusValidationException;
import org.apache.ignite.internal.catalog.IndexNotFoundValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.storage.StartBuildingIndexEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * Start building the index, switches from the {@link CatalogIndexStatus#REGISTERED} to the {@link CatalogIndexStatus#BUILDING}
 * {@link CatalogIndexDescriptor#status() status} in the catalog.
 *
 * @see CatalogIndexStatus
 * @see IndexNotFoundValidationException
 * @see ChangeIndexStatusValidationException
 */
public class StartBuildingIndexCommand implements CatalogCommand {
    /** Returns builder to start building index. */
    public static StartBuildingIndexCommandBuilder builder() {
        return new Builder();
    }

    private final int indexId;

    /** Constructor. */
    private StartBuildingIndexCommand(int indexId) {
        this.indexId = indexId;
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        CatalogIndexDescriptor index = indexOrThrow(catalog, indexId);

        if (index.status() != REGISTERED) {
            throw new ChangeIndexStatusValidationException(indexId, index.status(), BUILDING, REGISTERED);
        }

        return List.of(new StartBuildingIndexEntry(indexId));
    }

    private static class Builder implements StartBuildingIndexCommandBuilder {
        private int indexId;

        @Override
        public Builder indexId(int indexId) {
            this.indexId = indexId;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new StartBuildingIndexCommand(indexId);
        }
    }
}
