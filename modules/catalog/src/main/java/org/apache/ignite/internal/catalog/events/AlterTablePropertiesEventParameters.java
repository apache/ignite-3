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

package org.apache.ignite.internal.catalog.events;

import org.apache.ignite.internal.catalog.storage.AlterTablePropertiesEntry;
import org.jetbrains.annotations.Nullable;

/**
 * Alter table properties event parameters.
 */
public class AlterTablePropertiesEventParameters extends TableEventParameters {

    private final AlterTablePropertiesEntry entry;

    /**
     * Constructor.
     *
     * @param causalityToken Causality token.
     * @param catalogVersion Catalog version.
     * @param entry An entry which causes this event.
     */
    public AlterTablePropertiesEventParameters(
            long causalityToken,
            int catalogVersion,
            AlterTablePropertiesEntry entry
    ) {
        super(causalityToken, catalogVersion, entry.tableId());

        this.entry = entry;
    }

    /**
     * Returns {@code double} value in the range [0.0, 1] representing fraction of a partition to be modified before the data is considered
     * to be "stale", or {@code null} if this parameter didn't change.
     */
    public @Nullable Double staleRowsFraction() {
        return entry.staleRowsFraction();
    }

    /**
     * Returns minimal number of rows in partition to be modified before the data is considered to be "stale", or {@code null} if this
     * parameter didn't change.
     */
    public @Nullable Long minStaleRowsCount() {
        return entry.minStaleRowsCount();
    }
}
