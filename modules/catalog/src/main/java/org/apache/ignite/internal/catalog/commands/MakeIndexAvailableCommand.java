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
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.IndexAlreadyAvailableValidationException;
import org.apache.ignite.internal.catalog.IndexNotFoundValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.storage.MakeIndexAvailableEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;

/**
 * Makes the index available for read-write, switches from the write-only to the read-write state in the catalog.
 *
 * @see CatalogIndexDescriptor#writeOnly()
 * @see IndexNotFoundValidationException
 * @see IndexAlreadyAvailableValidationException
 */
public class MakeIndexAvailableCommand implements CatalogCommand {
    /** Returns builder to make an index available for read-write. */
    public static MakeIndexAvailableCommandBuilder builder() {
        return new Builder();
    }

    private final int indexId;

    /** Constructor. */
    private MakeIndexAvailableCommand(int indexId) {
        this.indexId = indexId;
    }

    @Override
    public List<UpdateEntry> get(Catalog catalog) {
        CatalogIndexDescriptor index = indexOrThrow(catalog, indexId);

        if (!index.writeOnly()) {
            throw new IndexAlreadyAvailableValidationException(format("Index is already available '{}'", indexId));
        }

        return List.of(new MakeIndexAvailableEntry(indexId));
    }

    private static class Builder implements MakeIndexAvailableCommandBuilder {
        private int indexId;

        @Override
        public Builder indexId(int indexId) {
            this.indexId = indexId;

            return this;
        }

        @Override
        public CatalogCommand build() {
            return new MakeIndexAvailableCommand(indexId);
        }
    }
}
