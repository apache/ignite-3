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
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.STOPPING;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.storage.RemoveIndexEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * A command that removes index with specified ID from the Catalog. Only makes sense for an index in the {@link CatalogIndexStatus#STOPPING}
 * state (so it's about removing a dropped index from the Catalog when we don't need it anymore).
 *
 * <p>This is always invoked as a reaction to an internal trigger, not directly by the end user.
 *
 * <p>For dropping an index, please refer to {@link DropIndexCommand}.
 *
 * @see DropIndexCommand
 */
public class RemoveIndexCommand implements CatalogCommand {
    /** Returns builder to create a command to remove index with specified name. */
    public static RemoveIndexCommandBuilder builder() {
        return new Builder();
    }

    private final int indexId;

    /**
     * Constructor.
     *
     * @param indexId ID of the index to remove.
     */
    private RemoveIndexCommand(int indexId) {
        this.indexId = indexId;
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        CatalogIndexDescriptor index = indexOrThrow(catalog, indexId);

        if (index.status() != STOPPING) {
            throw new CatalogValidationException("Cannot remove index {} because its status is {}", indexId, index.status());
        }

        return List.of(new RemoveIndexEntry(indexId));
    }

    private static class Builder implements RemoveIndexCommandBuilder {
        private int indexId;

        @Override
        public Builder indexId(int indexId) {
            this.indexId = indexId;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new RemoveIndexCommand(indexId);
        }
    }
}
